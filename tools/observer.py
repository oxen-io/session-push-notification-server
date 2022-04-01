import telepot
from telepot.loop import MessageLoop
from const import *
from utils import Singleton
from tools.lokiLogger import LokiLogger


class Observer(metaclass=Singleton):
    def __init__(self):
        self.logger = LokiLogger().logger
        self.bot = telepot.Bot("1685024629:AAHIvVUUdErsbtXW5UvoEw00GQM2TVTUFe8")
        self.subscribers = set()

        self.message_loop = MessageLoop(self.bot, self.handle)
        if not debug_mode:
            self.message_loop.run_as_thread(relax=1)

    def push_statistic_data(self, stats_data, now):
        info_string = f"Store data at {now}:\n" + stats_data.description()
        for chat_id in self.subscribers:
            self.bot.sendMessage(chat_id, info_string)

    def push_error(self, error_message):
        snippet = f'[Error]❌ {error_message}'
        for chat_id in self.subscribers:
            self.bot.sendMessage(chat_id, snippet)

    def push_warning(self, warning_message):
        snippet = f'[Warning]⚠️ {warning_message}'
        for chat_id in self.subscribers:
            self.bot.sendMessage(chat_id, snippet)

    def push_info(self, message):
        snippet = f'[Info]ℹ️ {message}'
        for chat_id in self.subscribers:
            self.bot.sendMessage(chat_id, snippet)

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



