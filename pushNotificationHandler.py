from logging.handlers import TimedRotatingFileHandler
from const import *
import os.path, asyncio, random, time, json, logging, collections
from threading import Thread
from PyAPNs.apns2.client import APNsClient, NotificationPriority, NotificationType, Notification
from PyAPNs.apns2.payload import Payload
from PyAPNs.apns2.errors import *


class SilentPushNotificationHelper:
    def __init__(self):
        self.apns = APNsClient(CERT_FILE, use_sandbox=False, use_alternative_port=False)
        self.thread = Thread(target=self.run_tasks)
        self.tokens = []
        self.push_fails = {}
        self.stop_running = False
        self.logger = logging.getLogger()
        self.log_config()
        self.load_tokens()

    def load_tokens(self):
        # TODO: Setup a DB?
        if os.path.isfile(TOKEN_DB):
            with open(TOKEN_DB, 'r') as token_db:
                self.tokens = list(json.load(token_db))
            token_db.close()

        for token in self.tokens:
            self.push_fails[token] = 0

    def log_config(self):
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        file_handler = TimedRotatingFileHandler('apns.log', 'midnight', 1, 0)
        file_handler.suffix = '%Y%m%d'
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def update_token(self, token):
        self.logger.info('update token ' + token)
        if token in self.tokens:
            return
        self.tokens.append(token)
        self.push_fails[token] = 0
        with open(TOKEN_DB, 'w') as token_db:
            token_db.write(json.dumps(self.tokens))
        token_db.close()

    async def create_tasks(self):
        task = asyncio.create_task(self.send_push_notification())
        await task

    def run_tasks(self):
        asyncio.run(self.create_tasks())

    def run(self):
        self.logger.info('Start running...')
        self.stop_running = False
        self.thread.start()

    def stop(self):
        self.logger.info('Stop running...')
        self.stop_running = True

    def handle_fail_result(self, token, result):
        if token in self.push_fails.keys():
            self.push_fails[token] += 1
        else:
            self.push_fails[token] = 1

        if self.push_fails[token] > 5:
            if token in self.tokens:
                self.tokens.remove(token)
                with open(TOKEN_DB, 'w') as token_db:
                    token_db.write(json.dumps(self.tokens))
                token_db.close()
            del self.push_fails[token]
        if isinstance(result, tuple):
            reason, info = result
            self.logger.warning("Push fail " + reason + ' ' + info + ' ' + token)
        else:
            self.logger.warning("Push fail for unknown reason " + token)

    def execute_push(self, tokens, payload):
        if len(tokens) == 0:
            return []

        retry_queue = []
        notifications = []
        results = {}
        for token in tokens:
            notifications.append(Notification(payload=payload, token=token))
        try:
            results = self.apns.send_notification_batch(notifications=notifications,
                                                        topic=BUNDLE_ID,
                                                        priority=NotificationPriority.Delayed)
        except ConnectionFailed:
            self.logger.error('Connection failed')
            self.apns = APNsClient(CERT_FILE, use_sandbox=False, use_alternative_port=False)
        except Exception as e:
            self.logger.exception(e)
            self.apns = APNsClient(CERT_FILE, use_sandbox=False, use_alternative_port=False)

        for token, result in results.items():
            if result != 'Success':
                retry_queue.append(token)
                self.handle_fail_result(token, result)
            else:
                self.push_fails[token] = 0

        return retry_queue

    async def send_push_notification(self):
        self.logger.info('Start to push')
        payload = Payload(content_available=True)
        retry_queue = []
        while True:
            random_sleep_time = random.randint(60, 180)
            self.logger.info('sleep for ' + str(random_sleep_time))
            for i in range(random_sleep_time):
                await asyncio.sleep(1)
                if random_sleep_time > 120 and i == 60:
                    self.logger.info('retry run at ' + time.asctime(time.localtime(time.time())) +
                                     ' for ' + str(len(retry_queue)) + ' tokens')
                    self.execute_push(retry_queue, payload)
                if self.stop_running:
                    return
            self.logger.info('push run at ' + time.asctime(time.localtime(time.time())) +
                             ' for ' + str(len(self.tokens)) + ' tokens')
            retry_queue = self.execute_push(self.tokens, payload)
