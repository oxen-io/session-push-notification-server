import asyncio
from aioapns import APNs, NotificationRequest, PushType, PRIORITY_HIGH
from utils import *
import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.exceptions import *
from model.pushNotificationStats import PushNotificationStats
from model.databaseModelV2 import *
from tools.databaseHelperV2 import DatabaseHelperV2
from tools.lokiLogger import LokiLogger
from queue import *


# PN approach V2 #
class PushNotificationHelperV2(metaclass=Singleton):
    # Init #
    def __init__(self):
        self.apns = APNs(client_cert=CERT_FILE, use_sandbox=debug_mode, topic=BUNDLE_ID)
        self.firebase_app = firebase_admin.initialize_app(credentials.Certificate(FIREBASE_TOKEN))
        self.push_fails = {}
        self.stats_data = PushNotificationStats()

        self.logger = LokiLogger().logger
        self.database_helper = DatabaseHelperV2()

        self.message_queue = Queue()

    # Registration #
    def remove_device_token(self, device_token):
        if device_token in self.push_fails.keys():
            del self.push_fails[device_token]
        if device_token in self.database_helper.token_device_mapping.keys():
            device = self.database_helper.token_device_mapping[device_token]
            device.remove_token(device_token)
            del self.database_helper.token_device_mapping[device_token]
            device.save_to_cache(self.database_helper)
            return device.session_id
        return None

    def register(self, device_token, session_id):
        self.remove_device_token(device_token)

        device = self.database_helper.get_device(session_id)
        # When there is no record for either the session id or the token
        if device is None:
            self.logger.info(f"New session id registered {session_id}.")
            device = Device()
            device.session_id = session_id
        else:
            self.logger.info(f"{session_id} registered a new device.")

        # When an existed session id adds a new device
        device.add_token(device_token)
        device.save_to_cache(self.database_helper)
        self.push_fails[device_token] = 0

    def unregister(self, device_token):
        session_id = self.remove_device_token(device_token)
        return session_id

    def subscribe_closed_group(self, closed_group_id, session_id):
        closed_group = self.database_helper.get_closed_group(closed_group_id)
        if closed_group is None:
            closed_group = ClosedGroup()
            closed_group.closed_group_id = closed_group_id
        closed_group.add_member(session_id)
        closed_group.save_to_cache(self.database_helper)

    def unsubscribe_closed_group(self, closed_group_id, session_id):
        closed_group = self.database_helper.get_closed_group(closed_group_id)
        if closed_group:
            closed_group.remove_member(session_id)
            closed_group.save_to_cache(self.database_helper)
            return closed_group_id
        return None

    # Notification #
    def add_message_to_queue(self, message):
        try:
            if debug_mode:
                self.logger.info("Adding new message to the message queue.")
            self.message_queue.put(message, timeout=5)
        except Full:
            self.logger.exception("Message queue is full.")
        except Exception as e:
            self.logger.exception(e)
            raise e

    async def send_push_notification(self):

        def generate_notifications(session_ids):
            for session_id in session_ids:
                device_for_push = self.database_helper.get_device(session_id)
                if device_for_push:
                    for device_token in device_for_push.tokens:
                        if is_ios_device_token(device_token):
                            alert = {'title': 'Session',
                                     'body': 'You\'ve got a new message'}
                            payload = {'alert': alert,
                                       'badge': 1,
                                       'sound': 'default',
                                       'mutable-content': 1,
                                       'category': 'SECRET'}
                            custom = {'ENCRYPTED_DATA': message['data'],
                                      'remote': 1}
                            payload.update(custom)
                            request = NotificationRequest(device_token=device_token,
                                                          message={'aps': payload},
                                                          priority=PRIORITY_HIGH,
                                                          push_type=PushType.ALERT)
                            notifications_ios.append(request)
                        else:
                            notification = messaging.Message(data={'ENCRYPTED_DATA': message['data']},
                                                             token=device_token,
                                                             android=messaging.AndroidConfig(priority='high'))
                            notifications_android.append(notification)

        if self.message_queue.empty():
            return
        # Get at most 1000 messages every 0.5 seconds
        messages_wait_to_push = []
        while not self.message_queue.empty() or len(messages_wait_to_push) > 1000:
            messages_wait_to_push.append(self.message_queue.get())

        self.stats_data.increment_total_message(len(messages_wait_to_push))
        notifications_ios = []
        notifications_android = []
        for message in messages_wait_to_push:
            recipient = message['send_to']
            device = self.database_helper.get_device(recipient)
            closed_group = self.database_helper.get_closed_group(recipient)
            if device:
                self.stats_data.increment_deduplicated_one_on_one_message(1)
                generate_notifications([recipient])
            elif closed_group:
                self.stats_data.increment_closed_group_message(1)
                generate_notifications(closed_group.members)
            else:
                self.stats_data.increment_untracked_message(1)
                if debug_mode:
                    self.logger.info(f'Ignore message to {recipient}.')
        try:
            await self.execute_push_ios(notifications_ios)
            self.execute_push_android(notifications_android)
        except Exception as e:
            self.logger.info('Something wrong happened when try to push notifications.')
            self.logger.exception(e)

    def execute_push_android(self, notifications):
        if len(notifications) == 0:
            return
        self.logger.info(f"Push {len(notifications)} notifications for Android.")
        self.stats_data.increment_android_pn(len(notifications))
        results = None
        try:
            results = messaging.send_all(messages=notifications, app=self.firebase_app)
        except FirebaseError as e:
            self.logger.error(e.cause)
        except Exception as e:
            self.logger.exception(e)

        if results is not None:
            for i in range(len(notifications)):
                response = results.responses[i]
                token = notifications[i].token
                if not response.success:
                    error = response.exception
                    self.logger.exception(error)
                    self.handle_fail_result(token, ('HttpError', ''))
                else:
                    self.push_fails[token] = 0

    async def execute_push_ios(self, notifications):
        if len(notifications) == 0:
            return
        self.logger.info(f"Push {len(notifications)} notifications for iOS.")
        self.stats_data.increment_ios_pn(len(notifications))
        for notification in notifications:
            response = await self.apns.send_notification(notification)
            if not response.is_successful:
                self.handle_fail_result(notification.device_token, (response.description, ''))
            else:
                self.push_fails[notification.device_token] = 0

    # Error handler #
    def handle_fail_result(self, key, result):
        if key in self.push_fails:
            self.push_fails[key] += 1
        else:
            self.push_fails[key] = 1

        if self.push_fails[key] > 5:
            self.remove_device_token(key)
        if isinstance(result, tuple):
            reason, info = result
            self.logger.warning(f"Push fail {reason} {info}.")
        else:
            self.logger.warning("Push fail for unknown reason.")
