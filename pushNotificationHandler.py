from const import *
from queue import *
import os.path
import asyncio
import pickle
from threading import Thread
from PyAPNs.apns2.client import APNsClient, NotificationPriority, Notification
from PyAPNs.apns2.payload import Payload, PayloadAlert
from PyAPNs.apns2.errors import *
from utils import *
import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.exceptions import *


# PN approach V2 #
class PushNotificationHelperV2:
    # Init #
    def __init__(self, logger):
        self.apns = APNsClient(CERT_FILE, use_sandbox=debug_mode, use_alternative_port=False)
        self.firebase_app = firebase_admin.initialize_app(credentials.Certificate(FIREBASE_TOKEN))
        self.device_token_map = {}  # {session_id: device_token}
        self.closed_group_map = {}  # {closed_group_id: [members session_ids]}
        self.message_queue = Queue()
        self.push_fails = {}
        self.logger = logger
        self.stop_running = False
        self.load_mappings()
        self.thread = Thread(target=self.run_push_notification_task)
        self.db_thread = Thread(target=self.run_sync_to_db_task)

    # Mapping #
    def load_mappings(self):
        self.logger.info("start to load tokens")
        if os.path.isfile(PUBKEY_TOKEN_DB_V2):
            with open(PUBKEY_TOKEN_DB_V2, 'rb') as pubkey_token_db:
                self.device_token_map = dict(pickle.load(pubkey_token_db))
            pubkey_token_db.close()

        for tokens in self.device_token_map.values():
            for token in tokens:
                self.push_fails[token] = 0

        self.logger.info("start to load closed groups")
        if os.path.isfile(CLOSED_GROUP_DB):
            with open(CLOSED_GROUP_DB, 'rb') as closed_group_db:
                self.closed_group_map = dict(pickle.load(closed_group_db))
            closed_group_db.close()

        self.logger.info("finish all loadings")

    def remove_device_token(self, device_token):
        if device_token in self.push_fails.keys():
            del self.push_fails[device_token]
        for session_id, tokens in self.device_token_map.copy().items():
            if device_token in tokens:
                self.device_token_map[session_id].remove(device_token)
                if len(self.device_token_map[session_id]) == 0:
                    self.device_token_map.pop(session_id)
                return session_id
        return 'No session id'

    # Sync mappings to local file #
    async def sync_to_db(self):
        while not self.stop_running:
            for i in range(600):
                await asyncio.sleep(1)
                if self.stop_running:
                    return

            self.logger.info('start to sync to file at ' + time.asctime(time.localtime(time.time())))
            self.logger.info('PN V2 session id numbers: ' + str(len(self.device_token_map.keys())))
            try:
                with open(PUBKEY_TOKEN_DB_V2, 'wb') as device_token_map_db:
                    pickle.dump(self.device_token_map, device_token_map_db)
                device_token_map_db.close()
                with open(CLOSED_GROUP_DB, 'wb') as closed_group_map_db:
                    pickle.dump(self.closed_group_map, closed_group_map_db)
                closed_group_map_db.close()
                self.logger.info('sync end at ' + time.asctime(time.localtime(time.time())))
            except Exception as e:
                self.logger.info('sync failed with error ' + str(type(e)))

    # Registration #
    def register(self, device_token, session_id):
        if session_id not in self.device_token_map.keys():
            self.logger.info("New session id registered " + session_id)
            self.device_token_map[session_id] = set()
        else:
            for key, tokens in self.device_token_map.copy().items():
                if key == session_id and device_token in tokens:
                    return
                if device_token in tokens:
                    self.device_token_map.pop(key)
                    self.device_token_map[session_id] = tokens

        self.device_token_map[session_id].add(device_token)
        self.push_fails[device_token] = 0

    def unregister(self, device_token):
        self.logger.info(self.remove_device_token(device_token) + " unregistered.")

    def subscribe_closed_group(self, closed_group_id, session_id):
        self.logger.info("New subscriber " + session_id + " to closed group " + closed_group_id)
        if closed_group_id not in self.closed_group_map:
            self.closed_group_map[closed_group_id] = set()
        self.closed_group_map[closed_group_id].add(session_id)

    def unsubscribe_closed_group(self, closed_group_id, session_id):
        if closed_group_id in self.closed_group_map:
            if session_id in self.closed_group_map[closed_group_id]:
                self.logger.info(session_id + " unsubscribe " + closed_group_id)
                self.closed_group_map[closed_group_id].remove(session_id)
                if len(self.closed_group_map[closed_group_id]) == 0:
                    del self.closed_group_map[closed_group_id]

    # Send PNs #
    def add_message_to_queue(self, message):
        try:
            if debug_mode:
                self.logger.info(message)
            self.message_queue.put(message, timeout=5)
        except Full:
            self.logger.exception("Message queue is full")
        except Exception as e:
            self.logger.exception(e)
            raise e

    async def loop_message_queue(self):
        while not self.stop_running:
            self.send_push_notification()
            await asyncio.sleep(1)

    def send_push_notification(self):
        if self.message_queue.empty() or self.stop_running:
            return
        messages_wait_to_push = []
        while not self.message_queue.empty() or len(messages_wait_to_push) > 1000:
            messages_wait_to_push.append(self.message_queue.get())

        def generate_notifications(session_ids):
            for session_id in session_ids:
                if session_id not in self.device_token_map.keys():
                    if debug_mode:
                        self.logger.info('Ignore closed group message to ' + recipient)
                    continue
                for device_token in self.device_token_map[session_id]:
                    if is_ios_device_token(device_token):
                        alert = PayloadAlert(title='Session', body='You\'ve got a new message')
                        payload = Payload(alert=alert, badge=1, sound="default",
                                          mutable_content=True, category="SECRET",
                                          custom={'ENCRYPTED_DATA': message['data']})
                        notifications_ios.append(Notification(token=device_token, payload=payload))
                    else:
                        notification = messaging.Message(data={'ENCRYPTED_DATA': message['data']},
                                                         token=device_token,
                                                         android=messaging.AndroidConfig(priority='high'))
                        notifications_android.append(notification)

        notifications_ios = []
        notifications_android = []
        for message in messages_wait_to_push:
            recipient = message['send_to']
            if recipient in self.device_token_map.keys():
                generate_notifications([recipient])
            elif recipient in self.closed_group_map.keys():
                generate_notifications(self.closed_group_map[recipient])
            else:
                if debug_mode:
                    self.logger.info('Ignore message to ' + recipient)
        self.execute_push_ios(notifications_ios, NotificationPriority.Immediate)
        self.execute_push_android(notifications_android)

    def execute_push_android(self, notifications):
        if len(notifications) == 0:
            return
        self.logger.info("Push " + str(len(notifications)) + " notifications for Android")
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
        self.logger.info("Push " + str(len(notifications)) + " notifications for iOS")
        results = {}
        try:
            results = self.apns.send_notification_batch(notifications=notifications, topic=BUNDLE_ID, priority=priority)
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
        task = asyncio.create_task(self.loop_message_queue())
        await task

    async def create_sync_to_db_task(self):
        task = asyncio.create_task(self.sync_to_db())
        await task

    def run_push_notification_task(self):
        asyncio.run(self.create_push_notification_task())

    def run_sync_to_db_task(self):
        asyncio.run(self.create_sync_to_db_task())

    def run(self):
        self.logger.info(self.__class__.__name__ + ' start running...')
        self.stop_running = False
        self.thread.start()
        self.db_thread.start()

    def stop(self):
        self.logger.info(self.__class__.__name__ + 'stop running...')
        self.stop_running = True

    # Error handler #
    def handle_fail_result(self, key, result):
        if key in self.push_fails.keys():
            self.push_fails[key] += 1
        else:
            self.push_fails[key] = 1

        if self.push_fails[key] > 5:
            self.remove_device_token(key)
            del self.push_fails[key]
        if isinstance(result, tuple):
            reason, info = result
            self.logger.warning("Push fail " + str(reason) + ' ' + str(info))
        else:
            self.logger.warning("Push fail for unknown reason")
