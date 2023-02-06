from datetime import datetime
import copy
import utils


class PushNotificationStats:

    class ResponseKey:
        DATA = 'data'
        IOS_DEVICE_NUMBER = 'ios_device_number'
        ANDROID_DEVICE_NUMBER = 'android_device_number'
        TOTAL_SESSION_ID_NUMBER = 'total_session_id_number'

    class Column:
        START_DATE = 'start_date'
        END_DATE = 'end_date'
        IOS_PN_NUMBER = 'ios_pn_number'
        ANDROID_PN_NUMBER = 'android_pn_number'
        TOTAL_MESSAGE_NUMBER = 'total_message_number'
        CLOSED_GROUP_MESSAGE_NUMBER = 'closed_group_message_number'
        UNTRACKED_MESSAGE_NUMBER = 'untracked_message_number'
        DEDUPLICATED_ONE_ON_ONE_MESSAGE_NUMBER = 'deduplicated_1_1_message_number'

    TABLE = 'statistics_table'
    COLUMNS = [Column.START_DATE, Column.END_DATE, Column.IOS_PN_NUMBER, Column.ANDROID_PN_NUMBER,
               Column.TOTAL_MESSAGE_NUMBER, Column.CLOSED_GROUP_MESSAGE_NUMBER, Column.UNTRACKED_MESSAGE_NUMBER,
               Column.DEDUPLICATED_ONE_ON_ONE_MESSAGE_NUMBER]
    CREATE_TABLE = (
        f'CREATE TABLE IF NOT EXISTS {TABLE} ('
        f'  {Column.START_DATE} REAL,'
        f'  {Column.END_DATE} REAL,'
        f'  {Column.IOS_PN_NUMBER} INTEGER,'
        f'  {Column.ANDROID_PN_NUMBER} INTEGER,'
        f'  {Column.TOTAL_MESSAGE_NUMBER} INTEGER,'
        f'  {Column.CLOSED_GROUP_MESSAGE_NUMBER} INTEGER,'
        f'  {Column.UNTRACKED_MESSAGE_NUMBER} INTEGER,'
        f'  {Column.DEDUPLICATED_ONE_ON_ONE_MESSAGE_NUMBER} INTEGER'
        f')'
    )

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
        return time_diff.days > 0

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
            PushNotificationStats.Column.START_DATE: utils.timestamp_to_formatted_date(row[0]),
            PushNotificationStats.Column.END_DATE: utils.timestamp_to_formatted_date(row[1]),
            PushNotificationStats.Column.IOS_PN_NUMBER: row[2],
            PushNotificationStats.Column.ANDROID_PN_NUMBER: row[3],
            PushNotificationStats.Column.TOTAL_MESSAGE_NUMBER: row[4],
            PushNotificationStats.Column.CLOSED_GROUP_MESSAGE_NUMBER: row[5],
            PushNotificationStats.Column.UNTRACKED_MESSAGE_NUMBER: row[6],
            PushNotificationStats.Column.DEDUPLICATED_ONE_ON_ONE_MESSAGE_NUMBER: row[7]
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
