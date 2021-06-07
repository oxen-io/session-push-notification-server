import telepot
from telepot.loop import MessageLoop
from datetime import datetime
import asyncio
from threading import Thread


class Observer:
    def __init__(self, logger):
        self.logger = logger
        self.bot = telepot.Bot("1685024629:AAHIvVUUdErsbtXW5UvoEw00GQM2TVTUFe8")
        self.last_ios_pn_number = 0
        self.last_android_pn_number = 0
        self.last_time_checked = None
        self.subscribers = []
        self.is_running = False
        self.thread = Thread(target=self.run_check_alive_task)
        self.message_loop = MessageLoop(self.bot, self.handle)

    def check_push_notification(self, ios_pn_number, android_pn_number):
        if ios_pn_number == self.last_ios_pn_number:
            for chat_id in self.subscribers:
                self.bot.sendMessage(chat_id, 'No new iOS PN during the last period. iOS PN might be crashed.')

        if android_pn_number == self.last_android_pn_number:
            for chat_id in self.subscribers:
                self.bot.sendMessage(chat_id, 'No new Android PN during the last period. Android PN might be crashed.')

        self.last_ios_pn_number = ios_pn_number
        self.last_android_pn_number = android_pn_number
        self.last_time_checked = datetime.now()

    def handle(self, message):
        content_type, chat_type, chat_id = telepot.glance(message)
        if content_type == 'text':
            if message['text'] == '/start':
                self.subscribers.append(chat_id)
                self.bot.sendMessage(chat_id, 'Start to observe PN server.')

    def run(self):
        self.is_running = True
        self.message_loop.run_as_thread()
        self.thread.start()

    def stop(self):
        self.is_running = False

    def run_check_alive_task(self):
        asyncio.run(self.create_check_alive_task())

    async def create_check_alive_task(self):
        while self.is_running:
            try:
                task = asyncio.create_task(self.check_alive())
                await task
            except Exception as e:
                self.logger.exception(e)

    async def check_alive(self):
        while self.is_running:
            now = datetime.now()
            time_diff = now - self.last_time_checked
            if time_diff.total_seconds() > 90:
                for chat_id in self.subscribers:
                    self.bot.sendMessage(chat_id, 'Not synced to DB for more than 90s. Process might be crashed.')
            await asyncio.sleep(10)

