from utils import *
from tools.pushNotificationHandler import PushNotificationHelperV2
from tools.databaseHelperV2 import DatabaseHelperV2
from tools.lokiLogger import LokiLogger
from tools.observer import Observer


class Tools(metaclass=Singleton):

    def __init__(self):
        self.notification_helper = PushNotificationHelperV2()
        self.database_helper = DatabaseHelperV2()
        self.logger = LokiLogger().logger
        self.observer = Observer()
        self.message_queue = Queue()
