from tasks.observeTask import ObserveTask
from tasks.syncDatabaseTask import SyncDatabaseTask
from tasks.pushNotificationTask import PushNotificationTask
from tasks.cleanDatabaseTask import CleanDatabaseTask


class TaskRunner:
    def __init__(self):
        self.tasks = [PushNotificationTask(), SyncDatabaseTask(), ObserveTask(), CleanDatabaseTask()]

    def run_tasks(self):
        for task in self.tasks:
            task.run()

    def stop_tasks(self):
        for task in self.tasks:
            task.stop()
