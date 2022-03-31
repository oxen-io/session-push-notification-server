from tasks.baseTask import *


class SyncDatabaseTask(BaseTask):
    def __init__(self):
        super().__init__()

        self.observer = Tools().observer
        self.database_helper = Tools().database_helper

    async def task(self):
        while self.is_running:
            try:
                for i in range(3 * 60):
                    await asyncio.sleep(1)
                    # Check should back up database every second
                    self.back_up_data_if_needed()
                # Flush cache to database every 3 minutes
                self.database_helper.flush_async()
                # Update stats data every 3 minutes
                self.store_data_if_needed()
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

    # TODO: Create a new row after 00:00 and update data every 3 minutes
    # Statistics #
    def store_data_if_needed(self):
        now = datetime.now()
        if self.notification_helper.stats_data.should_store_data(now):
            self.logger.info(f"Store data at {now}:\n" + self.notification_helper.stats_data.description())
            current_data = self.notification_helper.stats_data.copy()
            self.notification_helper.stats_data.reset(now)
            self.database_helper.store_stats_data_async(current_data)
            self.observer.push_statistic_data(current_data, now)