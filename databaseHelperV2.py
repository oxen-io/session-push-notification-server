import sqlite3
import utils
from datetime import datetime
from const import *
from databaseModelV2 import *
from pushNotificationStats import PushNotificationStats
from utils import TaskQueue


class DatabaseHelperV2:
    def __init__(self, logger):
        self.logger = logger
        self.last_backup = datetime.now()
        self.task_queue = TaskQueue()
        self.device_cache = {}  # {session_id: Device}
        self.token_device_mapping = {}  # {token: Device}
        self.closed_group_cache = {}  # {closed_group_id: ClosedGroup}
        self.create_tables_if_needed()
        self.migrate()

    # Database backup
    def should_back_up_database(self, now):
        time_diff = now - self.last_backup
        return time_diff.total_seconds() >= 24 * 60 * 60

    def back_up_database_async(self):
        self.task_queue.add_task(self.back_up_database)

    def back_up_database(self):
        self.logger.info(f"Start to backup database at {datetime.now()}.")
        db_connection = sqlite3.connect(DATABASE_V2)
        backup_db_connection = sqlite3.connect(DATABASE_V2_BACKUP)
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
        db_connection = sqlite3.connect(DATABASE_V2)
        cursor = db_connection.cursor()
        cursor.execute(SQLStatements.CREATE_DEVICE_TOKEN_MAPPING_TABLE)
        cursor.execute(SQLStatements.CREATE_CLOSED_GROUP_MEMBER_MAPPING_TABLE)
        cursor.execute(SQLStatements.CREATE_STATISTICS_DATA_TABLE)
        db_connection.commit()
        cursor.close()
        db_connection.close()

    def migrate(self):
        db_connection = sqlite3.connect(DATABASE_V2)
        cursor = db_connection.cursor()
        cursor.execute(SQLStatements.INSERT_DEVICE_TYPE_COLUMN_INTO_DEVICE_TOKEN_MAPPING_TABLE)
        db_connection.commit()
        cursor.close()
        db_connection.close()

    def populate_cache(self):
        db_connection = sqlite3.connect(DATABASE_V2)
        cursor = db_connection.cursor()

        # Populate device token mapping cache
        query = SQLStatements.FETCH.format('*', PUBKEY_TOKEN_TABLE)
        cursor.execute(query)
        device_token_rows = cursor.fetchall()
        for row in device_token_rows:
            session_id = row[0]
            token = Device.Token(row[1], row[2])
            device = self.get_device(session_id) or Device(session_id)
            device.tokens.add(token)  # Won't trigger needs_to_be_updated
            self.device_cache[session_id] = device
            self.token_device_mapping[token.value] = device

        # Populate closed group members mapping cache
        query = SQLStatements.FETCH.format('*', CLOSED_GROUP_TABLE) + f''
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
        self.logger.info(f"Start to sync to DB at {datetime.now()}.")
        db_connection = sqlite3.connect(DATABASE_V2)
        cursor = db_connection.cursor()

        def batch_update(table, key, cache, value_count):
            rows_to_update = list()
            for item in cache.values():
                if item.needs_to_be_updated:
                    rows_to_update += item.to_database_rows()
            query = SQLStatements.DELETE.format(table) + f'WHERE {key} = ?'
            cursor.executemany(query, [(row[0],) for row in rows_to_update])
            statement = SQLStatements.NEW.format(table, ','.join('?' * value_count))
            cursor.executemany(statement, rows_to_update)

        try:
            # Update device token into database
            batch_update(PUBKEY_TOKEN_TABLE, PUBKEY, self.device_cache, 3)

            # Update closed group into database
            batch_update(CLOSED_GROUP_TABLE, CLOSED_GROUP, self.closed_group_cache, 2)

            db_connection.commit()
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
    def store_stats_data_async(self, stats_data):
        self.task_queue.add_task(self.store_stats_data, stats_data)

    def store_stats_data(self, stats_data):
        db_connection = sqlite3.connect(DATABASE_V2)
        cursor = db_connection.cursor()
        statement = SQLStatements.NEW.format(STATISTICS_TABLE, ','.join('?' * 8))
        cursor.execute(statement, stats_data.to_database_row())
        db_connection.commit()
        cursor.close()
        db_connection.close()

    def get_stats_data(self, start_date, end_date):
        db_connection = sqlite3.connect(DATABASE_V2)
        cursor = db_connection.cursor()
        statement = SQLStatements.FETCH.format('*', STATISTICS_TABLE)
        if start_date:
            statement += f'WHERE {START_DATE} >= {utils.formatted_date_to_timestamp(start_date)}'
            if end_date:
                statement += f' AND {END_DATE} <= {utils.formatted_date_to_timestamp(end_date)}'
        cursor.execute(statement)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            data.append(PushNotificationStats.from_database_row(row))

        # Get device number
        ios, android, total = self.get_device_number()

        # Final result
        result = {DATA: data,
                  IOS_DEVICE_NUMBER: ios,
                  ANDROID_DEVICE_NUMBER: android,
                  TOTAL_SESSION_ID_NUMBER: total}

        cursor.close()
        db_connection.close()
        return result

    def get_device_number(self):
        ios, android, total = 0, 0, 0
        for session_id, device in self.device_cache.items():
            if len(device.tokens) > 0:
                total += 1
                for token in device.tokens:
                    if utils.is_ios_device_token(token):
                        ios += 1
                    else:
                        android += 1
        return ios, android, total


class SQLStatements:
    # MARK: Create tables
    CREATE_DEVICE_TOKEN_MAPPING_TABLE = (
        f'CREATE TABLE IF NOT EXISTS {PUBKEY_TOKEN_TABLE} ('
        f'  {PUBKEY} TEXT NOT NULL,'
        f'  {TOKEN} TEXT NOT NULL'
        f')'
    )
    CREATE_CLOSED_GROUP_MEMBER_MAPPING_TABLE = (
        f'CREATE TABLE IF NOT EXISTS {CLOSED_GROUP_TABLE} ('
        f'  {CLOSED_GROUP} TEXT NOT NULL,'
        f'  {PUBKEY} TEXT NOT NULL'
        f')'
    )
    CREATE_STATISTICS_DATA_TABLE = (
        f'CREATE TABLE IF NOT EXISTS {STATISTICS_TABLE} ('
        f'  {START_DATE} REAL,'
        f'  {END_DATE} REAL,'
        f'  {IOS_PN_NUMBER} INTEGER,'
        f'  {ANDROID_PN_NUMBER} INTEGER,'
        f'  {TOTAL_MESSAGE_NUMBER} INTEGER,'
        f'  {CLOSED_GROUP_MESSAGE_NUMBER} INTEGER,'
        f'  {UNTRACKED_MESSAGE_NUMBER} INTEGER,'
        f'  {DEDUPLICATED_ONE_ON_ONE_MESSAGE_NUMBER} INTEGER'
        f')'
    )

    # MARK: Fetch
    FETCH = 'SELECT {} FROM {} '
    # MARK: Update
    NEW = 'INSERT INTO {} VALUES ({}) '
    DELETE = 'DELETE FROM {} '

    INSERT_DEVICE_TYPE_COLUMN_INTO_DEVICE_TOKEN_MAPPING_TABLE = (
        f'ALTER TABLE {PUBKEY_TOKEN_TABLE}'
        f' ADD {DEVICE_TYPE} TEXT'
    )
