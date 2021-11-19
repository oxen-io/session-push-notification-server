from datetime import datetime
import copy


class PushNotificationStats(object):
    # Init #
    def __init__(self):
        self.last_statistics_date = datetime.now()
        self.total_messages = 0
        self.notification_counter_ios = 0
        self.notification_counter_android = 0
        self.closed_group_messages = 0
        self.untracked_messages = 0
        self.deduplicated_one_on_one_messages = 0

    def description(self):
        return f"iOS push notification number: {self.notification_counter_ios}\n" + \
               f"Android push notification number: {self.notification_counter_android}\n" + \
               f"Closed group message number: {self.closed_group_messages}\n" + \
               f"Total message number: {self.total_messages}\n" + \
               f"Untracked message number: {self.untracked_messages}\n" + \
               f"Deduplicated 1-1 message number: {self.deduplicated_one_on_one_messages}\n"

    def copy(self):
        return copy.deepcopy(self)

    def should_store_data(self, now):
        time_diff = now - self.last_statistics_date
        return time_diff.total_seconds() >= 12 * 60 * 60

    def reset(self, now):
        self.last_statistics_date = now
        self.total_messages = 0
        self.notification_counter_ios = 0
        self.notification_counter_android = 0
        self.closed_group_messages = 0
        self.untracked_messages = 0
        self.deduplicated_one_on_one_messages = 0

    # Incremental #
    def increment_total_message(self, number):
        self.total_messages += number

    def increment_ios_pn(self, number):
        self.notification_counter_ios += number

    def increment_android_pn(self, number):
        self.notification_counter_android += number

    def increment_closed_group_message(self, number):
        self.closed_group_messages += number

    def increment_untracked_message(self, number):
        self.untracked_messages += number

    def increment_deduplicated_one_on_one_message(self, number):
        self.deduplicated_one_on_one_messages += number
