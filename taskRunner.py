from const import debug_mode
from queue import Full
from toolManager import Tools
from tasks.observeTask import ObserveTask
from tasks.syncDatabaseTask import SyncDatabaseTask
from tasks.pushNotificationTask import PushNotificationTask


class TaskRunner:
    def __init__(self):
        self.logger = Tools().logger
        self.message_queue = Tools().message_queue

        self.tasks = [PushNotificationTask(), SyncDatabaseTask(), ObserveTask()]

    def run_tasks(self):
        for task in self.tasks:
            task.run()

    def stop_tasks(self):
        for task in self.tasks:
            task.stop()

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
