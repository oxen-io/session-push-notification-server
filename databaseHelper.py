import time
from json import JSONDecodeError

import utils
from const import *
from tinydb import TinyDB, Query
from datetime import datetime
from threading import Lock
import pickle
import os


class DatabaseModel:
    def __init__(self, table, doc_id=None):
        self.table = table
        self.doc_id = doc_id
        self.need_to_save = False

    def from_mapping(self, mapping):
        pass

    def to_mapping(self):
        pass

    def save(self, database_helper):
        self.need_to_save = True

    def saved_to_db(self, doc_id):
        if doc_id:
            self.doc_id = doc_id
            self.need_to_save = False


class Device(DatabaseModel):
    def __init__(self, doc_id=None, session_id=None, tokens=None):
        super().__init__(PUBKEY_TOKEN_TABLE, doc_id)
        self.session_id = session_id
        self.tokens = set(tokens) if tokens else set()

    def from_mapping(self, mapping):
        self.session_id = mapping[PUBKEY]
        self.tokens = set(mapping[TOKEN])

    def to_mapping(self):
        return {PUBKEY: self.session_id,
                TOKEN: list(self.tokens)}

    def save(self, database_helper):
        database_helper.device_cache[self.session_id] = self
        for token in self.tokens:
            database_helper.token_device_mapping[token] = self
        super().save(database_helper)


class ClosedGroup(DatabaseModel):
    def __init__(self, doc_id=None, closed_group_id=None, members=None):
        super().__init__(CLOSED_GROUP_TABLE, doc_id)
        self.closed_group_id = closed_group_id
        self.members = set(members) if members else set()

    def from_mapping(self, mapping):
        self.closed_group_id = mapping[CLOSED_GROUP]
        self.members = set(mapping[MEMBERS])

    def to_mapping(self):
        return {CLOSED_GROUP: self.closed_group_id,
                MEMBERS: list(self.members)}

    def save(self, database_helper):
        database_helper.closed_group_cache[self.closed_group_id] = self
        super().save(database_helper)


class DatabaseHelper:
    def __init__(self, database=DATABASE):
        self.is_flushing = False
        self.tinyDB = TinyDB(database, ensure_ascii=False)
        self.device_cache = {}  # {session_id: Device}
        self.token_device_mapping = {}  # {token: Device}
        self.closed_group_cache = {}  # {closed_group_id: ClosedGroup}
        self.mutex = Lock()

    def load_cache(self):
        device_table = self.tinyDB.table(PUBKEY_TOKEN_TABLE)
        devices = device_table.all()
        need_to_remove = []
        for device_mapping in devices:
            if device_mapping[PUBKEY]:
                device = Device(doc_id=device_mapping.doc_id)
                device.from_mapping(device_mapping)
                self.device_cache[device.session_id] = device
                for token in device.tokens:
                    self.token_device_mapping[token] = device
            else:
                need_to_remove.append(device_mapping.doc_id)
        device_table.remove(doc_ids=need_to_remove)

        closed_group_table = self.tinyDB.table(CLOSED_GROUP_TABLE)
        closed_groups = closed_group_table.all()
        need_to_remove = []
        for closed_group_mapping in closed_groups:
            if closed_group_mapping[CLOSED_GROUP]:
                closed_group = ClosedGroup(doc_id=closed_group_mapping.doc_id)
                closed_group.from_mapping(closed_group_mapping)
                self.closed_group_cache[closed_group.closed_group_id] = closed_group
            else:
                need_to_remove.append(closed_group_mapping.doc_id)
        closed_group_table.remove(doc_ids=need_to_remove)

    def flush(self):
        def batch_flush(items, table):
            items_need_to_save = []
            items_need_to_update = []
            mappings = []
            for item in items:
                if item.need_to_save:
                    items_need_to_save.append(item)
                    mappings.append(item.to_mapping())
                    if item.doc_id:
                        items_need_to_update.append(item.doc_id)
            self.tinyDB.table(table).remove(doc_ids=items_need_to_update)
            doc_ids = self.tinyDB.table(table).insert_multiple(mappings)
            for i in range(len(items_need_to_save)):
                items_need_to_save[i].saved_to_db(doc_ids[i])

        if self.is_flushing:
            return
        try:
            self.mutex.acquire(True, 60)
            self.is_flushing = True
            batch_flush(self.device_cache.values(), PUBKEY_TOKEN_TABLE)
            batch_flush(self.closed_group_cache.values(), CLOSED_GROUP_TABLE)
            self.is_flushing = False
            self.mutex.release()
        except Exception as e:
            self.is_flushing = False
            self.mutex.release()
            raise e

    def migrate_database_if_needed(self):

        def migrate(old_db_name, new_table_name, json_structure):
            db_map = None
            if os.path.isfile(old_db_name):
                with open(old_db_name, 'rb') as old_db:
                    db_map = dict(pickle.load(old_db))
                old_db.close()
            if db_map is not None and len(db_map) > 0:
                items = []
                for key, value in db_map.items():
                    item = {}
                    for key_name, value_name in json_structure.items():
                        item[key_name] = key
                        item[value_name] = list(value)
                    items.append(item)
                self.tinyDB.table(new_table_name).insert_multiple(items)
                os.remove(old_db_name)

        migrate(PUBKEY_TOKEN_DB_V2, PUBKEY_TOKEN_TABLE, {PUBKEY: TOKEN})
        migrate(CLOSED_GROUP_DB, CLOSED_GROUP_TABLE, {CLOSED_GROUP: MEMBERS})

    def store_data(self, last_statistics_date, now, ios_pn_number, android_pn_number, total_message_number, closed_group_message_number):
        self.mutex.acquire(True, 60)
        db = self.tinyDB.table(STATISTICS_TABLE)
        fmt = "%Y-%m-%d %H:%M:%S"
        db.insert({START_DATE: last_statistics_date.strftime(fmt),
                   END_DATE: now.strftime(fmt),
                   IOS_PN_NUMBER: ios_pn_number,
                   ANDROID_PN_NUMBER: android_pn_number,
                   TOTAL_MESSAGE_NUMBER: total_message_number,
                   CLOSED_GROUP_MESSAGE_NUMBER: closed_group_message_number})
        self.mutex.release()

    def get_data(self, start_date, end_date):
        db = self.tinyDB.table(STATISTICS_TABLE)

        def try_to_convert_datetime(date_str):
            formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    pass

        def test_func(val, date_str, ascending):
            date_1 = try_to_convert_datetime(val)
            date_2 = try_to_convert_datetime(date_str)
            return date_1 > date_2 if ascending else date_1 < date_2

        def get_statistics_data():
            result = None
            self.mutex.acquire(True, 60)
            try:
                data_query = Query()
                if start_date and end_date:
                    data = db.search(data_query[START_DATE].test(test_func, start_date, True) &
                                     data_query[END_DATE].test(test_func, end_date, False))
                elif start_date:
                    data = db.search(data_query[START_DATE].test(test_func, start_date, True))
                else:
                    data = db.all()
                ios_device_number = 0
                android_device_number = 0
                total_session_id_number = 0
                for session_id, device in self.device_cache.items():
                    if len(device.tokens) > 0:
                        total_session_id_number += 1
                        for token in device.tokens:
                            if utils.is_ios_device_token(token):
                                ios_device_number += 1
                            else:
                                android_device_number += 1

                result = {DATA: data,
                          IOS_DEVICE_NUMBER: ios_device_number,
                          ANDROID_DEVICE_NUMBER: android_device_number,
                          TOTAL_SESSION_ID_NUMBER: total_session_id_number}
            except JSONDecodeError as e:
                self.correct_database(e.pos)

            self.mutex.release()
            return result

        final_result = get_statistics_data()
        retry = 0
        while final_result is None and retry < 10:
            final_result = get_statistics_data()
            retry += 1
        return final_result

    def get_device(self, session_id):
        return self.device_cache.get(session_id, None)

    def get_closed_group(self, closed_group_id):
        return self.closed_group_cache.get(closed_group_id, None)

    def correct_database(self, index):
        if self.is_flushing:
            return
        if os.path.isfile(DATABASE):
            with open(DATABASE, 'rb') as db:
                string = db.read()
            db.close()
            with open(DATABASE, 'wb') as db:
                db.write(string[:index])
            db.close()

