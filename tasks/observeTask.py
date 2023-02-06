from tasks.baseTask import *
from const import Environment
from tools.observer import Observer
from tools.pushNotificationHandler import PushNotificationHelperV2
from tools.databaseHelperV2 import DatabaseHelperV2
from datetime import datetime


class ObserveTask(BaseTask):
    def __init__(self):
        super().__init__()

        self.observer = Observer()
        self.stats_data = PushNotificationHelperV2().stats_data
        self.database_helper = DatabaseHelperV2()

        self.last_ios_pn_number = 0
        self.last_android_pn_number = 0

    async def task(self):
        while self.is_running:
            await asyncio.sleep(5 * 60)
            if self.database_helper.last_flush:
                now = datetime.now()
                time_diff = now - self.database_helper.last_flush
                if time_diff.total_seconds() > 300:
                    self.observer.push_warning('Not synced to DB for more than 5 min. Process might be crashed.')
                self.check_push_notification()

    def check_push_notification(self):
        if self.stats_data.notification_counter_ios == self.last_ios_pn_number and not Environment.debug_mode:
            self.observer.push_warning('No new iOS PN during the last period. iOS PN might be crashed.')

        if self.stats_data.notification_counter_android == self.last_android_pn_number and not Environment.debug_mode:
            self.observer.push_warning('No new Android PN during the last period. Android PN might be crashed.')

        self.last_ios_pn_number = self.stats_data.notification_counter_ios
        self.last_android_pn_number = self.stats_data.notification_counter_android
        self.logger.info('Check alive.')
