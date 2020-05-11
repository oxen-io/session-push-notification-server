from const import *
import os.path, asyncio, random, pickle
from threading import Thread
from PyAPNs.apns2.client import APNsClient, NotificationPriority, Notification
from PyAPNs.apns2.payload import Payload, PayloadAlert
from PyAPNs.apns2.errors import *
from lokiAPI import LokiAPI
from utils import *
import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.exceptions import  *


class PushNotificationHelper:
    def __init__(self, logger):
        self.apns = APNsClient(CERT_FILE, use_sandbox=debug_mode, use_alternative_port=False)
        self.firebase_app = None
        self.thread = Thread(target=self.run_tasks)
        self.push_fails = {}
        self.logger = logger
        self.stop_running = False
        self.load_tokens()

    def load_tokens(self):
        # TODO: Setup a DB?
        pass

    async def send_push_notification(self):
        pass

    async def create_tasks(self):
        task = asyncio.create_task(self.send_push_notification())
        await task

    def run_tasks(self):
        asyncio.run(self.create_tasks())

    def execute_push_Android(self, notifications):
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

    def execute_push_iOS(self, notifications, priority):
        if len(notifications) == 0:
            return
        self.logger.info("Push " + str(len(notifications)) + " notifications for iOS")
        results = {}
        try:
            results = self.apns.send_notification_batch(notifications=notifications, topic=BUNDLE_ID, priority=priority)
        except ConnectionFailed:
            self.logger.error('Connection failed')
            self.execute_push_iOS(notifications, priority)
        except Exception as e:
            self.logger.exception(e)
            self.execute_push_iOS(notifications, priority)
        for token, result in results.items():
            if result != 'Success':
                self.handle_fail_result(token, result)
            else:
                self.push_fails[token] = 0

    def run(self):
        self.logger.info(self.__class__.__name__ + ' start running...')
        self.stop_running = False
        self.thread.start()

    def stop(self):
        self.logger.info(self.__class__.__name__ + 'stop running...')
        self.stop_running = True

    def handle_fail_result(self, key, result):
        if key in self.push_fails.keys():
            self.push_fails[key] += 1
        else:
            self.push_fails[key] = 1

        if self.push_fails[key] > 5:
            self.remove_invalid_token(key)
            del self.push_fails[key]
        if isinstance(result, tuple):
            reason, info = result
            self.logger.warning("Push fail " + str(reason) + ' ' + str(info))
        else:
            self.logger.warning("Push fail for unknown reason")

    def disable_token(self, token):
        self.remove_invalid_token(token)
        if token in self.push_fails.keys():
            del self.push_fails[token]

    def remove_invalid_token(self, token):
        pass


class SilentPushNotificationHelper(PushNotificationHelper):
    def __init__(self, logger):
        self.tokens = []
        super().__init__(logger)

    def load_tokens(self):
        if os.path.isfile(TOKEN_DB):
            with open(TOKEN_DB, 'rb') as token_db:
                self.tokens = list(pickle.load(token_db))
            token_db.close()

        for token in self.tokens:
            self.push_fails[token] = 0

    def update_token(self, token):
        if token in self.tokens or not is_iOS_device_token(token):
            return
        self.tokens.append(token)
        self.push_fails[token] = 0
        with open(TOKEN_DB, 'wb') as token_db:
            pickle.dump(self.tokens, token_db)
        token_db.close()

    def remove_invalid_token(self, token):
        if token in self.tokens:
            self.tokens.remove(token)
            with open(TOKEN_DB, 'wb') as token_db:
                pickle.dump(self.tokens, token_db)
            token_db.close()

    async def send_push_notification(self):
        self.logger.info('Start to push')
        payload = Payload(content_available=True)
        while True:
            random_sleep_time = random.randint(60, 180)
            self.logger.info('sleep for ' + str(random_sleep_time))
            for i in range(random_sleep_time):
                await asyncio.sleep(1)
                if self.stop_running:
                    return
            self.logger.info('push run at ' + time.asctime(time.localtime(time.time())) +
                             ' for ' + str(len(self.tokens)) + ' tokens')
            notifications = []
            for token in self.tokens:
                notifications.append(Notification(payload=payload, token=token))
            self.execute_push_iOS(notifications, NotificationPriority.Delayed)


class NormalPushNotificationHelper(PushNotificationHelper):
    def __init__(self, logger):
        self.api = LokiAPI(logger)
        self.pubkey_token_dict = {}
        self.last_hash = {}
        super().__init__(logger)
        self.firebase_app = firebase_admin.initialize_app(credentials.Certificate(FIREBASE_TOKEN))
        self.db_thread = Thread(target=self.run_sync_db_tasks)

    def run(self):
        super().run()
        self.db_thread.start()

    def load_tokens(self):
        self.logger.info("start to load tokens")
        if os.path.isfile(PUBKEY_TOKEN_DB):
            with open(PUBKEY_TOKEN_DB, 'rb') as pubkey_token_db:
                self.pubkey_token_dict = dict(pickle.load(pubkey_token_db))
            pubkey_token_db.close()

        for tokens in self.pubkey_token_dict.values():
            for token in tokens:
                self.push_fails[token] = 0

        self.logger.info("start to load last hash")
        if os.path.isfile(LAST_HASH_DB):
            with open(LAST_HASH_DB, 'rb') as last_hash_db:
                self.last_hash = dict(pickle.load(last_hash_db))
            last_hash_db.close()
        for pubkey in self.pubkey_token_dict.keys():
            if pubkey not in self.last_hash.keys():
                self.last_hash[pubkey] = {LASTHASH: '',
                                          EXPIRATION: 0}

        self.logger.info("start to load swarms")
        if os.path.isfile(SWARM_DB):
            with open(SWARM_DB, 'rb') as swarm_db:
                self.api.swarm_cache = dict(pickle.load(swarm_db))
            swarm_db.close()
        self.logger.info("finish all loadings")
        self.api.init_for_swarms(list(self.pubkey_token_dict.keys()))

    def update_last_hash(self, pubkey, last_hash, expiration):
        expiration = process_expiration(expiration)
        if pubkey in self.last_hash.keys():
            if self.last_hash[pubkey][EXPIRATION] < expiration:
                self.last_hash[pubkey] = {LASTHASH: last_hash,
                                          EXPIRATION: expiration}

    def update_token_pubkey_pair(self, token, pubkey):
        if pubkey not in self.pubkey_token_dict.keys():
            self.logger.info("New session id registered " + pubkey)
            self.pubkey_token_dict[pubkey] = set()
            self.api.init_for_swarms([pubkey])
        else:
            for key, tokens in self.pubkey_token_dict.items():
                if key == pubkey and token in tokens:
                    return
                if token in tokens:
                    self.pubkey_token_dict.pop(key)
                    self.pubkey_token_dict[pubkey] = tokens

        self.pubkey_token_dict[pubkey].add(token)
        self.push_fails[token] = 0
        self.last_hash[pubkey] = {LASTHASH: '',
                                  EXPIRATION: 0}

    def remove_invalid_token(self, token):
        for pubkey, tokens in self.pubkey_token_dict.items():
            if token in tokens:
                self.pubkey_token_dict[pubkey].remove(token)
                if len(self.pubkey_token_dict[pubkey]) == 0:
                    self.pubkey_token_dict.pop(pubkey)
                break

    async def create_sync_db_tasks(self):
        task = asyncio.create_task(self.sync_to_db())
        await task

    def run_sync_db_tasks(self):
        asyncio.run(self.create_sync_db_tasks())

    async def sync_to_db(self):
        while not self.stop_running:
            for i in range(600):
                await asyncio.sleep(1)
                if self.stop_running:
                    return
            self.logger.info('start to sync to file at ' + time.asctime(time.localtime(time.time())))
            try:
                with open(PUBKEY_TOKEN_DB, 'wb') as pubkey_token_db:
                    pickle.dump(self.pubkey_token_dict, pubkey_token_db)
                pubkey_token_db.close()
                with open(LAST_HASH_DB, 'wb') as last_hash_db:
                    pickle.dump(self.last_hash, last_hash_db)
                last_hash_db.close()
                with open(SWARM_DB, 'wb') as swarm_db:
                    pickle.dump(self.api.swarm_cache, swarm_db)
                swarm_db.close()
                self.logger.info('sync end at ' + time.asctime(time.localtime(time.time())))
            except Exception as e:
                self.logger.info('sync failed with error ' + str(type(e)))

    async def fetch_messages(self):
        self.logger.info('fetch run at ' + time.asctime(time.localtime(time.time())) +
                         ' for ' + str(len(self.pubkey_token_dict.keys())) + ' pubkeys')
        return self.api.fetch_raw_messages(list(self.pubkey_token_dict.keys()), self.last_hash)

    async def send_push_notification(self):
        while not self.api.is_ready:
            await asyncio.sleep(1)
        self.logger.info('Start to fetch and push')
        while not self.stop_running:
            notifications_iOS = []
            notifications_Android = []
            raw_messages = await self.fetch_messages()
            for pubkey, messages in raw_messages.items():
                if len(messages) == 0:
                    continue
                for message in messages:
                    if pubkey not in self.pubkey_token_dict.keys():
                        continue
                    message_expiration = process_expiration(message['expiration'])
                    current_time = int(round(time.time() * 1000))
                    if message_expiration - current_time < 23.8 * 60 * 60 * 1000:
                        continue
                    for token in self.pubkey_token_dict[pubkey]:
                        self.logger.info("New PN to " + pubkey)
                        if is_iOS_device_token(token):
                            alert = PayloadAlert(title='Session', body='You\'ve got a new message')
                            payload = Payload(alert=alert, badge=1, sound="default",
                                              mutable_content=True, category="SECRET",
                                              custom={'ENCRYPTED_DATA': message['data']})
                            notifications_iOS.append(Notification(token=token, payload=payload))
                        else:
                            notification = messaging.Message(data={'ENCRYPTED_DATA': message['data']},
                                                             token=token)
                            notifications_Android.append(notification)
                last_message = messages[len(messages) - 1]
                message_expiration = process_expiration(last_message['expiration'])
                if self.last_hash[pubkey][LASTHASH] != last_message['hash']:
                    self.logger.info("Update last hash for" + pubkey)
                    self.last_hash[pubkey] = {LASTHASH: last_message['hash'],
                                              EXPIRATION: message_expiration}
            self.execute_push_iOS(notifications_iOS, NotificationPriority.Immediate)
            self.execute_push_Android(notifications_Android)

