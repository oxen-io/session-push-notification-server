from datetime import datetime
import copy
from const import *
import utils


class PushNotificationStats:
    # Init #
    def __init__(self):
        self.start_date = datetime.now()
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

    def should_create_new_entry(self, now):
        time_diff = now - self.start_date
        return time_diff.days() > 0

    def reset(self, now):
        self.start_date = now
        self.total_messages = 0
        self.notification_counter_ios = 0
        self.notification_counter_android = 0
        self.closed_group_messages = 0
        self.untracked_messages = 0
        self.deduplicated_one_on_one_messages = 0

    def to_database_row(self):
        return (self.start_date.timestamp(),
                datetime.now().timestamp(),
                self.notification_counter_ios,
                self.notification_counter_android,
                self.total_messages,
                self.closed_group_messages,
                self.untracked_messages,
                self.deduplicated_one_on_one_messages)

    @classmethod
    def from_database_row(cls, row):
        return {
            START_DATE: utils.timestamp_to_formatted_date(row[0]),
            END_DATE: utils.timestamp_to_formatted_date(row[1]),
            IOS_PN_NUMBER: row[2],
            ANDROID_PN_NUMBER: row[3],
            TOTAL_MESSAGE_NUMBER: row[4],
            CLOSED_GROUP_MESSAGE_NUMBER: row[5],
            UNTRACKED_MESSAGE_NUMBER: row[6],
            DEDUPLICATED_ONE_ON_ONE_MESSAGE_NUMBER: row[7]
        }

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
