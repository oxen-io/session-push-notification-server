from logging.handlers import TimedRotatingFileHandler
from const import *
import os.path, asyncio, random, time, json, logging
from threading import Thread
from PyAPNs.apns2.client import APNsClient, NotificationPriority, Notification
from PyAPNs.apns2.payload import Payload
from PyAPNs.apns2.errors import *
from lokiAPI import LokiAPI


class PushNotificationHelper:
    def __init__(self):
        self.apns = APNsClient(CERT_FILE, use_sandbox=debug_mode, use_alternative_port=False)
        self.thread = Thread(target=self.run_tasks)
        self.push_fails = {}
        self.stop_running = False
        self.logger = logging.getLogger()
        self.log_config()
        self.load_tokens()

    def load_tokens(self):
        # TODO: Setup a DB?
        pass

    def log_config(self):
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        file_handler = TimedRotatingFileHandler('apns.log', 'midnight', 1, 0)
        file_handler.suffix = '%Y%m%d'
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    async def send_push_notification(self):
        pass

    async def create_tasks(self):
        task = asyncio.create_task(self.send_push_notification())
        await task

    def run_tasks(self):
        asyncio.run(self.create_tasks())

    def execute_push(self, notifications, priority):
        if len(notifications) == 0:
            return []

        results = {}
        try:
            results = self.apns.send_notification_batch(notifications=notifications,
                                                        topic=BUNDLE_ID,
                                                        priority=priority)
        except ConnectionFailed:
            self.logger.error('Connection failed')
            self.apns = APNsClient(CERT_FILE, use_sandbox=False, use_alternative_port=False)
        except Exception as e:
            self.logger.exception(e)
            self.apns = APNsClient(CERT_FILE, use_sandbox=False, use_alternative_port=False)

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
            self.logger.warning("Push fail " + reason + ' ' + info)
        else:
            self.logger.warning("Push fail for unknown reason")

    def remove_invalid_token(self, token):
        pass


class SilentPushNotificationHelper(PushNotificationHelper):
    def __init__(self):
        self.tokens = []
        super().__init__()

    def load_tokens(self):
        if os.path.isfile(TOKEN_DB):
            with open(TOKEN_DB, 'r') as token_db:
                self.tokens = list(json.load(token_db))
            token_db.close()

        for token in self.tokens:
            self.push_fails[token] = 0

    def update_token(self, token):
        self.logger.info('update token ' + token)
        if token in self.tokens:
            return
        self.tokens.append(token)
        self.push_fails[token] = 0
        with open(TOKEN_DB, 'w') as token_db:
            token_db.write(json.dumps(self.tokens))
        token_db.close()

    def remove_invalid_token(self, token):
        if token in self.tokens:
            self.tokens.remove(token)
            with open(TOKEN_DB, 'w') as token_db:
                token_db.write(json.dumps(self.tokens))
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
            self.execute_push(notifications, NotificationPriority.Delayed)


class NormalPushNotificationHelper(PushNotificationHelper):
    def __init__(self):
        self.pubkey_token_dict = {}
        super().__init__()
        self.api = LokiAPI()

    def load_tokens(self):
        if os.path.isfile(PUBKEY_TOKEN_DB):
            with open(PUBKEY_TOKEN_DB, 'r') as pubkey_token_db:
                self.pubkey_token_dict = dict(json.load(pubkey_token_db))
            pubkey_token_db.close()

        for tokens in self.pubkey_token_dict.values():
            for token in tokens:
                self.push_fails[token] = 0

    def update_token_pubkey_pair(self, token, pubkey):
        self.logger.info('update token pubkey pairs (' + token + ', ' + pubkey + ')')
        if pubkey not in self.pubkey_token_dict.keys():
            self.pubkey_token_dict[pubkey] = set()
        else:
            for key, tokens in self.pubkey_token_dict.items():
                if key == pubkey and token in tokens:
                    return
                if token in tokens:
                    self.pubkey_token_dict.pop(key)
                    self.pubkey_token_dict[pubkey] = tokens

        self.pubkey_token_dict[pubkey].add(token)
        self.push_fails[token] = 0
        with open(PUBKEY_TOKEN_DB, 'w') as pubkey_token_db:
            pubkey_token_db.write(json.dumps(self.pubkey_token_dict))
        pubkey_token_db.close()

    def remove_invalid_token(self, token):
        for pubkey, tokens in self.pubkey_token_dict.items():
            if token in tokens:
                self.pubkey_token_dict[pubkey].remove(token)
                break
            with open(PUBKEY_TOKEN_DB, 'w') as pubkey_token_db:
                pubkey_token_db.write(json.dumps(self.pubkey_token_dict))
            pubkey_token_db.close()

    async def fetch_messages(self):
        self.logger.info('fetch run at ' + time.asctime(time.localtime(time.time())) +
                         ' for ' + str(len(self.pubkey_token_dict.keys())) + ' pubkeys')
        return self.api.fetch_raw_messages(self.pubkey_token_dict.keys())

    async def send_push_notification(self):
        self.logger.info('Start to fetch and push')
        while not self.stop_running:
            notifications = []
            start_fetching_time = int(round(time.time()))
            raw_messages = await self.fetch_messages()
            fetching_time = int(round(time.time())) - start_fetching_time
            waiting_time = 60 - fetching_time
            if waiting_time < 0:
                self.logger.warning('Fetching messages over 60 seconds')
            else:
                for i in range(waiting_time):
                    await asyncio.sleep(1)
                    if self.stop_running:
                        return
            for pubkey, messages in raw_messages:
                for message in messages:
                    payload = Payload(alert='ENCRYPTED MESSAGE', badge=1, sound="default",
                                      mutable_content=True, category="SECRET",
                                      custom={'ENCRYPTED_DATA': message})
                    for token in self.pubkey_token_dict[pubkey]:
                        notifications.append(Notification(token=token, payload=payload))
            self.execute_push(notifications, NotificationPriority.Immediate)
