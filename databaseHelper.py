from const import *
from tinydb import TinyDB, where, Query
from datetime import datetime


class DatabaseModel:
    def __init__(self, database, doc_id=None):
        self.database = database
        self.doc_id = doc_id

    def find(self, queries):
        final_query = None
        for query in queries:
            final_query = final_query & query if final_query else query
        if final_query:
            documents = self.database.search(final_query)
            if len(documents) > 0:
                self.doc_id = documents[0].doc_id
                self.from_mapping(documents[0])
                return True
        return False

    def from_mapping(self, mapping):
        pass

    def to_mapping(self):
        pass

    def save(self):
        mapping = self.to_mapping()
        if self.doc_id:
            self.database.update(mapping, doc_ids=[self.doc_id])
        else:
            self.doc_id = self.database.insert(mapping)


class Device(DatabaseModel):
    def __init__(self, database, doc_id=None, session_id=None, tokens=None):
        super().__init__(database.table(PUBKEY_TOKEN_TABLE), doc_id)
        if session_id:
            self.session_id = session_id
            documents = self.database.search(where(PUBKEY) == session_id)
            if len(documents) > 0:
                self.doc_id = documents[0].doc_id
        self.tokens = set(tokens) if tokens else set()

    def from_mapping(self, mapping):
        self.session_id = mapping[PUBKEY]
        self.tokens = set(mapping[TOKEN])

    def to_mapping(self):
        return {PUBKEY: self.session_id,
                TOKEN: list(self.tokens)}


class ClosedGroup(DatabaseModel):
    def __init__(self, database, doc_id=None, closed_group_id=None, members=None):
        super().__init__(database.table(CLOSED_GROUP_TABLE), doc_id)
        if closed_group_id:
            self.closed_group_id = closed_group_id
            documents = self.database.search(where(CLOSED_GROUP) == closed_group_id)
            if len(documents) > 0:
                self.doc_id = documents[0].doc_id
        self.members = set(members) if members else set()

    def from_mapping(self, mapping):
        self.closed_group_id = mapping[CLOSED_GROUP]
        self.members = set(mapping[MEMBERS])

    def to_mapping(self):
        return {CLOSED_GROUP: self.closed_group_id,
                MEMBERS: list(self.members)}


def get_data(start_date, end_date):
    database = TinyDB(DATABASE).table(STATISTICS_TABLE)

    def test_func(val, date_str, ascending):
        fmt = "%Y-%m-%d %H:%M:%S"
        date_1 = datetime.strptime(val, fmt)
        date_2 = datetime.strptime(date_str, fmt)
        return date_1 > date_2 if ascending else date_1 < date_2

    data_query = Query()
    if start_date and end_date:
        return database.search(data_query[START_DATE].test(test_func, start_date, True) &
                               data_query[END_DATE].test(test_func, end_date, False))
    elif start_date:
        return database.search(data_query[START_DATE].test(test_func, start_date, True))
    else:
        return database.all()
