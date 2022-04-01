from tasks.baseTask import *
from const import debug_mode
from tools.observer import Observer
from tools.pushNotificationHandler import PushNotificationHelperV2
from datetime import datetime


class ObserveTask(BaseTask):
    def __init__(self):
        super().__init__()

        self.observer = Observer()
        self.stats_data = PushNotificationHelperV2().stats_data

        self.last_ios_pn_number = 0
        self.last_android_pn_number = 0
        self.last_time_checked = None

    async def task(self):
        while self.is_running:
            if self.last_time_checked:
                now = datetime.now()
                time_diff = now - self.last_time_checked
                if time_diff.total_seconds() > 300:
                    self.observer.push_warning('Not synced to DB for more than 5 min. Process might be crashed.')
            await asyncio.sleep(10)

    # TODO: New ways to observe if a database flush is done in every 5 minutes
    def check_push_notification(self):
        if self.stats_data.notification_counter_ios == self.last_ios_pn_number and not debug_mode:
            self.observer.push_warning('No new iOS PN during the last period. iOS PN might be crashed.')

        if self.stats_data.notification_counter_android == self.last_android_pn_number and not debug_mode:
            self.observer.push_warning('No new Android PN during the last period. Android PN might be crashed.')

        self.last_ios_pn_number = self.stats_data.notification_counter_ios
        self.last_android_pn_number = self.stats_data.notification_counter_android
        self.last_time_checked = datetime.now()
        self.logger.info('Check alive.')
