from tasks.observeTask import ObserveTask
from tasks.syncDatabaseTask import SyncDatabaseTask
from tasks.pushNotificationTask import PushNotificationTask
from tools.lokiLogger import LokiLogger


class TaskRunner:
    def __init__(self):
        self.tasks = [PushNotificationTask(), SyncDatabaseTask(), ObserveTask()]

    def run_tasks(self):
        for task in self.tasks:
            task.run()

    def stop_tasks(self):
        for task in self.tasks:
            task.stop()
