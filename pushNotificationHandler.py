from const import *
import os.path, asyncio, random, time, json, logging
from threading import Thread
from PyAPNs.apns2.client import APNsClient, NotificationPriority, NotificationType
from PyAPNs.apns2.payload import Payload


class SilentPushNotificationHelper:
    def __init__(self):
        self.apns = APNsClient(CERT_FILE, use_sandbox=True, use_alternative_port=False)
        self.thread = Thread(target=self.run_tasks)
        self.tokens = []
        self.stop_running = False
        self.logger = logging.getLogger()
        self.log_config()
        # TODO: Setup a DB?
        if os.path.isfile(TOKEN_DB):
            with open(TOKEN_DB, 'r') as token_db:
                self.tokens = list(json.load(token_db))
            token_db.close()

    def log_config(self):
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(str(int(time.time())) + '_push.log')
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def update_token(self, token):
        self.logger.info('update token ' + token)
        if token in self.tokens:
            return
        self.tokens.append(token)
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

    async def send_push_notification(self):
        self.logger.info('Start to push')
        while True:
            # random_sleep_time = random.randint(600, 1800)
            random_sleep_time = random.randint(60, 180)
            self.logger.info('send sleep at ' + time.asctime(time.localtime(time.time())) + ' for ' + str(random_sleep_time))
            for i in range(random_sleep_time):
                await asyncio.sleep(1)
                if self.stop_running:
                    return
            self.logger.info('send run at ' + time.asctime(time.localtime(time.time())))
            if len(self.tokens) > 0:
                # payload = Payload(alert="Ryan Test", sound="default", badge=1)
                payload = Payload(content_available=True)
                for token in self.tokens:
                    self.logger.info('PUSH NOTIFICATION TO ' + token)
                    stream_id = self.apns.send_notification_async(token, payload,
                                                                  topic=BUNDLE_ID,
                                                                  priority=NotificationPriority.Delayed,
                                                                  push_type=NotificationType.Background)
                    result = self.apns.get_notification_result(stream_id)
                    if result != 'Success':
                        if isinstance(result, tuple):
                            reason, info = result
                            self.logger.warning("Push fail", reason, info)
                        else:
                            self.logger.warning("Push fail for unknown reason")
                            # If push fails with unknown reason, just delete the token
                            # TODO: Test to see if this is correct
                            self.tokens.remove(token)
                            with open(TOKEN_DB, 'w') as token_db:
                                token_db.write(json.dumps(self.tokens))
                            token_db.close()



