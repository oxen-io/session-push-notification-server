from tasks.baseTask import *
from tools.observer import Observer
from tools.databaseHelperV2 import DatabaseHelperV2
from tools.pushNotificationHandler import PushNotificationHelperV2
from datetime import datetime


class SyncDatabaseTask(BaseTask):
    def __init__(self):
        super().__init__()

        self.observer = Observer()
        self.database_helper = DatabaseHelperV2()
        self.notification_helper = PushNotificationHelperV2()

    async def task(self):
        while self.is_running:
            try:
                for i in range(3 * 60):
                    await asyncio.sleep(1)
                    # Check should back up database every second
                    self.back_up_data_if_needed()
                    self.create_new_stats_data_entry_if_needed()
                # Flush cache to database every 3 minutes
                self.database_helper.flush_async()
                # Update stats data every 3 minutes
                self.persist_stats_data()
            except Exception as e:
                error_message = f"Flush exception: {e}"
                self.logger.error(error_message)
                self.observer.push_error(error_message)

    # Database backup #
    def back_up_data_if_needed(self):
        now = datetime.now()
        if self.database_helper.should_back_up_database(now):
            info = f"Back up database at {now}.\n"
            self.logger.info(info)
            self.database_helper.back_up_database_async()
            self.database_helper.last_backup = now
            self.observer.push_info(info)

    # Create a new row after 00:00 and update data every 3 minutes
    # Statistics #
    def create_new_stats_data_entry_if_needed(self):
        now = datetime.now()
        if self.notification_helper.stats_data.should_create_new_entry(now):
            self.persist_stats_data()
            self.observer.push_statistic_data(self.notification_helper.stats_data, now)
            self.notification_helper.stats_data.reset(now)
            self.database_helper.create_new_entry_for_stats_data_async(self.notification_helper.stats_data.copy())

    def persist_stats_data(self):
        current_data = self.notification_helper.stats_data.copy()
        self.database_helper.store_stats_data_async(current_data)

