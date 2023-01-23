from queue import *
import asyncio
from threading import Thread
from PyAPNs.apns2.client import APNsClient, NotificationPriority, Notification, NotificationType
from PyAPNs.apns2.payload import Payload, PayloadAlert
from PyAPNs.apns2.errors import *
from utils import *
import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.exceptions import *
from databaseModelV2 import *
from databaseHelperV2 import *
from pushNotificationStats import *


# PN approach V2 #
class PushNotificationHelperV2:
    # Init #
    def __init__(self, logger, database_helper, observer):
        self.apns = APNsClient(CERT_FILE, use_sandbox=debug_mode, use_alternative_port=False)
        self.firebase_app = firebase_admin.initialize_app(credentials.Certificate(FIREBASE_TOKEN))
        self.message_queue = Queue()
        self.push_fails = {}
        self.logger = logger
        self.database_helper = database_helper
        self.observer = observer
        self.stop_running = False
        self.thread = Thread(target=self.run_push_notification_task)
        self.db_thread = Thread(target=self.run_sync_to_db_task)
        self.stats_data = PushNotificationStats()

    # Statistics #
    def store_data_if_needed(self):
        now = datetime.now()
        if self.stats_data.should_store_data(now):
            self.logger.info(f"Store data at {now}:\n" + self.stats_data.description())
            current_data = self.stats_data.copy()
            self.stats_data.reset(now)
            self.database_helper.store_stats_data_async(current_data)
            self.observer.push_statistic_data(current_data, now)

    # Database backup #
    def back_up_data_if_needed(self):
        now = datetime.now()
        if self.database_helper.should_back_up_database(now):
            info = f"Back up database at {now}.\n"
            self.logger.info(info)
            self.database_helper.back_up_database_async()
            self.database_helper.last_backup = now
            self.observer.push_info(info)

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

    # Sync mappings to local file #
    async def sync_to_db(self):
        while not self.stop_running:
            try:
                for i in range(3 * 60):
                    await asyncio.sleep(1)
                    # Check should store stats data & should back up database every second
                    self.store_data_if_needed()
                    self.back_up_data_if_needed()
                    if self.stop_running:
                        return
                self.observer.check_push_notification(self.stats_data)
                # Flush cache to database every 3 minutes
                self.database_helper.flush_async()
            except Exception as e:
                error_message = f"Flush exception: {e}"
                self.logger.error(error_message)
                self.observer.push_error(error_message)

    # Send PNs #
    def add_message_to_queue(self, message):
        try:
            if debug_mode:
                self.logger.info(message)
            self.message_queue.put(message, timeout=5)
        except Full:
            self.logger.exception("Message queue is full.")
        except Exception as e:
            self.logger.exception(e)
            raise e

    async def loop_message_queue(self):
        while not self.stop_running:
            self.send_push_notification()
            await asyncio.sleep(0.5)

    def send_push_notification(self):
        if self.message_queue.empty() or self.stop_running:
            return
        # Get at most 1000 messages every second
        messages_wait_to_push = []
        while not self.message_queue.empty() or len(messages_wait_to_push) > 1000:
            messages_wait_to_push.append(self.message_queue.get())

        def generate_notifications(session_ids):
            for session_id in session_ids:
                device_for_push = self.database_helper.get_device(session_id)
                if device_for_push:
                    for device_token in device_for_push.tokens:
                        if is_ios_device_token(device_token):
                            alert = PayloadAlert(title='Session', body='You\'ve got a new message')
                            payload = Payload(alert=alert, badge=1, sound="default",
                                              mutable_content=True, category="SECRET",
                                              custom={'ENCRYPTED_DATA': message['data'],
                                                      'remote': 1})
                            notifications_ios.append(Notification(token=device_token, payload=payload))
                        else:
                            notification = messaging.Message(data={'ENCRYPTED_DATA': message['data']},
                                                             token=device_token,
                                                             android=messaging.AndroidConfig(priority='high'))
                            notifications_android.append(notification)

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
            self.execute_push_ios(notifications_ios, NotificationPriority.Immediate)
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
                    self.handle_fail_result(token, ("HttpError", ""))
                else:
                    self.push_fails[token] = 0

    def execute_push_ios(self, notifications, priority):
        if len(notifications) == 0:
            return
        self.logger.info(f"Push {len(notifications)} notifications for iOS.")
        self.stats_data.increment_ios_pn(len(notifications))
        results = {}
        try:
            results = self.apns.send_notification_batch(notifications=notifications, topic=BUNDLE_ID,
                                                        priority=priority, push_type=NotificationType.Alert)
        except ConnectionFailed:
            self.logger.error('Connection failed')
            self.execute_push_ios(notifications, priority)
        except Exception as e:
            self.logger.exception(e)
            self.execute_push_ios(notifications, priority)
        for token, result in results.items():
            if result != 'Success':
                self.handle_fail_result(token, result)
            else:
                self.push_fails[token] = 0

    # Tasks #
    async def create_push_notification_task(self):
        while not self.stop_running:
            try:
                task = asyncio.create_task(self.loop_message_queue())
                await task
            except Exception as e:
                self.logger.exception(e)
                self.logger.warning('Push Notification Task has stopped, restart now.')
        self.logger.info('Push Notification Task has stopped.')

    async def create_sync_to_db_task(self):
        task = asyncio.create_task(self.sync_to_db())
        await task

    def run_push_notification_task(self):
        asyncio.run(self.create_push_notification_task())

    def run_sync_to_db_task(self):
        asyncio.run(self.create_sync_to_db_task())

    def run(self):
        self.logger.info(f'{self.__class__.__name__} start running...')
        self.stop_running = False
        self.thread.start()
        self.db_thread.start()

    def stop(self):
        self.logger.info(f'{self.__class__.__name__} stop running...')
        self.stop_running = True
        self.database_helper.flush()

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
