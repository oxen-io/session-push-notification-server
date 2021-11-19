import telepot
from telepot.loop import MessageLoop
from datetime import datetime
import asyncio
from threading import Thread
from const import *


class Observer:
    def __init__(self, logger):
        self.logger = logger
        self.bot = telepot.Bot("1685024629:AAHIvVUUdErsbtXW5UvoEw00GQM2TVTUFe8")
        self.last_ios_pn_number = 0
        self.last_android_pn_number = 0
        self.last_time_checked = None
        self.subscribers = set()
        self.is_running = False
        self.thread = Thread(target=self.run_check_alive_task)
        self.message_loop = MessageLoop(self.bot, self.handle)

    def check_push_notification(self, stats_data):
        if stats_data.notification_counter_ios == self.last_ios_pn_number and not debug_mode:
            for chat_id in self.subscribers:
                self.bot.sendMessage(chat_id, 'No new iOS PN during the last period. iOS PN might be crashed.')

        if stats_data.notification_counter_android == self.last_android_pn_number and not debug_mode:
            for chat_id in self.subscribers:
                self.bot.sendMessage(chat_id, 'No new Android PN during the last period. Android PN might be crashed.')

        self.last_ios_pn_number = stats_data.notification_counter_ios
        self.last_android_pn_number = stats_data.notification_counter_android
        self.last_time_checked = datetime.now()
        self.logger.info('Check alive.')

    def push_statistic_data(self, stats_data, now):
        info_string = f"Store data at {now}:\n" + stats_data.description()
        for chat_id in self.subscribers:
            self.bot.sendMessage(chat_id, info_string)

    def handle(self, message):
        content_type, chat_type, chat_id = telepot.glance(message)
        if content_type == 'text':
            if debug_mode:
                self.bot.sendMessage(chat_id, 'Debug mode. You won\'t get observer messages.')
                return
            
            if message['text'] == '/start':
                self.subscribers.add(chat_id)
                self.bot.sendMessage(chat_id, 'Start to observe PN server.')

            if message['text'] == '/stop':
                self.subscribers.remove(chat_id)
                self.bot.sendMessage(chat_id, 'Stop to observe PN server.')

    def run(self):
        self.is_running = True
        if not debug_mode:
            self.message_loop.run_as_thread(relax=1)
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
            if self.last_time_checked:
                now = datetime.now()
                time_diff = now - self.last_time_checked
                if time_diff.total_seconds() > 300:
                    for chat_id in self.subscribers:
                        self.bot.sendMessage(chat_id, 'Not synced to DB for more than 5 min. Process might be crashed.')
            await asyncio.sleep(10)

