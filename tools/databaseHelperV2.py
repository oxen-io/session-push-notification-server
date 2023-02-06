import sqlite3
import utils
from datetime import datetime
from model.databaseModelV2 import Device, ClosedGroup
from model.pushNotificationStats import PushNotificationStats
from utils import TaskQueue, Singleton, DeviceType
from tools.lokiLogger import LokiLogger


class DatabaseHelperV2(metaclass=Singleton):
    def __init__(self):
        self.database = 'session_pn_server.db'
        self.backup_database = 'session_pn_server_backup.db'
        self.logger = LokiLogger().logger
        self.last_backup = datetime.now()
        self.last_flush = None
        self.task_queue = TaskQueue()
        self.device_cache = {}  # {session_id: Device}
        self.token_device_mapping = {}  # {token: Device}
        self.closed_group_cache = {}  # {closed_group_id: ClosedGroup}
        self.create_tables_if_needed()
        self.migration_device_type()
        self.populate_cache()

    # Database backup
    def should_back_up_database(self, now):
        time_diff = now - self.last_backup
        return time_diff.total_seconds() >= 24 * 60 * 60

    def back_up_database_async(self):
        self.task_queue.add_task(self.back_up_database)

    def back_up_database(self):
        self.logger.info(f"Start to backup database at {datetime.now()}.")
        db_connection = sqlite3.connect(self.database)
        backup_db_connection = sqlite3.connect(self.backup_database)
        try:
            with backup_db_connection:
                db_connection.backup(backup_db_connection)
        except Exception as e:
            error_message = f"Database backup exception: {e}"
            self.logger.error(error_message)
        finally:
            backup_db_connection.close()
            db_connection.close()
            self.logger.info(f"Finish to backup database at {datetime.now()}.")

    def create_tables_if_needed(self):
        db_connection = sqlite3.connect(self.database)
        cursor = db_connection.cursor()
        cursor.execute(Device.CREATE_TABLE)
        cursor.execute(ClosedGroup.CREATE_TABLE)
        cursor.execute(PushNotificationStats.CREATE_TABLE)
        db_connection.commit()
        cursor.close()
        db_connection.close()

    def migration_device_type(self):
        db_connection = sqlite3.connect(self.database)
        cursor = db_connection.cursor()
        try:
            cursor.execute(Device.INSERT_DEVICE_TOKEN)
            db_connection.commit()
        except Exception as e:
            self.logger.error(e)
        cursor.close()
        db_connection.close()

    def populate_cache(self):
        db_connection = sqlite3.connect(self.database)
        cursor = db_connection.cursor()

        # Populate device token mapping cache
        query = SQL.FETCH.format('*', Device.TABLE)
        cursor.execute(query)
        device_token_rows = cursor.fetchall()
        for row in device_token_rows:
            session_id = row[0]
            device_type = DeviceType(row[2]) if row[2] is not None else None
            token = Device.Token(row[1], device_type)
            device = self.get_device(session_id) or Device(session_id)
            device.tokens.add(token)  # Won't trigger needs_to_be_updated
            self.device_cache[session_id] = device
            self.token_device_mapping[token.value] = device

        # Populate closed group members mapping cache
        query = SQL.FETCH.format('*', ClosedGroup.TABLE) + f''
        cursor.execute(query)
        closed_group_rows = cursor.fetchall()
        for row in closed_group_rows:
            closed_group_id = row[0]
            member_id = row[1]
            closed_group = self.get_closed_group(closed_group_id) or ClosedGroup(closed_group_id)
            closed_group.members.add(member_id)   # Won't trigger needs_to_be_updated
            self.closed_group_cache[closed_group_id] = closed_group

        cursor.close()
        db_connection.close()

    def flush_async(self):
        if self.task_queue.empty():
            self.task_queue.add_task(self.flush)

    def flush(self):
        now = datetime.now()
        self.logger.info(f"Start to sync to DB at {now}.")
        db_connection = sqlite3.connect(self.database)
        cursor = db_connection.cursor()

        def batch_update(table, key, cache, value_count):
            rows_to_update = list()
            for item in cache.values():
                if item.needs_to_be_updated:
                    rows_to_update += item.to_database_rows()
            query = SQL.DELETE.format(table) + f'WHERE {key} = ?'
            cursor.executemany(query, [(row[0],) for row in rows_to_update])
            statement = SQL.NEW.format(table, ','.join('?' * value_count))
            cursor.executemany(statement, rows_to_update)
            self.logger.info(f"{len(rows_to_update)} rows have been updated.")

        try:
            # Update device token into database
            batch_update(Device.TABLE, Device.Column.PUBKEY, self.device_cache, len(Device.COLUMNS))

            # Update closed group into database
            batch_update(ClosedGroup.TABLE, ClosedGroup.Column.CLOSED_GROUP, self.closed_group_cache, len(ClosedGroup.COLUMNS))

            db_connection.commit()

            self.last_flush = now
        except Exception as e:
            error_message = f"Flush exception: {e}"
            self.logger.error(error_message)
        finally:
            cursor.close()
            db_connection.close()
            self.logger.info(f"End of flush at {datetime.now()}.")

    def get_device(self, session_id):
        return self.device_cache.get(session_id, None)

    def get_closed_group(self, closed_group_id):
        return self.closed_group_cache.get(closed_group_id, None)

    # Statistics
    def create_new_entry_for_stats_data_async(self, stats_data):
        self.task_queue.add_task(self.create_new_entry_for_stats_data, stats_data)

    def create_new_entry_for_stats_data(self, stats_data):
        db_connection = sqlite3.connect(self.database)
        cursor = db_connection.cursor()
        statement = SQL.NEW.format(PushNotificationStats.TABLE, ','.join('?' * 8))
        cursor.execute(statement, stats_data.to_database_row())
        db_connection.commit()
        cursor.close()
        db_connection.close()

    def store_stats_data_async(self, stats_data):
        self.task_queue.add_task(self.store_stats_data, stats_data)

    def store_stats_data(self, stats_data):
        db_connection = sqlite3.connect(self.database)
        cursor = db_connection.cursor()
        statement = SQL.DELETE.format(PushNotificationStats.TABLE) + f'WHERE {PushNotificationStats.Column.START_DATE} = {stats_data.start_date.timestamp()}'
        cursor.execute(statement)
        statement = SQL.NEW.format(PushNotificationStats.TABLE, ','.join('?' * 8))
        cursor.execute(statement, stats_data.to_database_row())
        db_connection.commit()
        cursor.close()
        db_connection.close()

    def get_stats_data(self, start_date, end_date):
        db_connection = sqlite3.connect(self.database)
        cursor = db_connection.cursor()
        statement = SQL.FETCH.format('*', PushNotificationStats.TABLE)
        if start_date:
            statement += f'WHERE {PushNotificationStats.Column.START_DATE} >= {utils.formatted_date_to_timestamp(start_date)}'
            if end_date:
                statement += f' AND {PushNotificationStats.Column.END_DATE} <= {utils.formatted_date_to_timestamp(end_date)}'
        cursor.execute(statement)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            data.append(PushNotificationStats.from_database_row(row))

        # Get device number
        ios, android, total = self.get_device_number()

        # Final result
        result = {PushNotificationStats.ResponseKey.DATA: data,
                  PushNotificationStats.ResponseKey.IOS_DEVICE_NUMBER: ios,
                  PushNotificationStats.ResponseKey.ANDROID_DEVICE_NUMBER: android,
                  PushNotificationStats.ResponseKey.TOTAL_SESSION_ID_NUMBER: total}

        cursor.close()
        db_connection.close()
        return result

    def get_device_number(self):
        ios, android, total = 0, 0, 0
        for session_id, device in self.device_cache.items():
            if len(device.tokens) > 0:
                total += 1
                for token in device.tokens:
                    if token.device_type == DeviceType.iOS:
                        ios += 1
                    elif token.device_type == DeviceType.Android or token.device_type == DeviceType.Huawei:
                        android += 1
        return ios, android, total


class SQL:
    # MARK: Fetch
    FETCH = 'SELECT {} FROM {} '
    # MARK: Update
    NEW = 'INSERT INTO {} VALUES ({}) '
    DELETE = 'DELETE FROM {} '

