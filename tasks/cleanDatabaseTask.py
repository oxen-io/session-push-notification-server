from tasks.baseTask import *
from tools.observer import Observer
from tools.databaseHelperV2 import DatabaseHelperV2
from tools.pushNotificationHandler import PushNotificationHelperV2


class CleanDatabaseTask(BaseTask):
    def __init__(self):
        super().__init__()

        self.observer = Observer()
        self.database_helper = DatabaseHelperV2()
        self.notification_helper = PushNotificationHelperV2()

    async def task(self):
        while self.is_running:
            try:
                await asyncio.sleep(30 * 24 * 60 * 60)
                zombies = len(self.database_helper.device_cache.keys()) - len(self.notification_helper.latest_activity_timestamp.keys())
                self.observer.push_info(f"In last 30 days, {zombies} users have been marked as zombies.")
                self.notification_helper.latest_activity_timestamp = {}
            except Exception as e:
                error_message = f"Flush exception: {e}"
                self.logger.error(error_message)
