from utils import DeviceType, is_ios_device_token
from tools.lokiLogger import LokiLogger


class Device:

    class Column:
        PUBKEY = 'pubKey'
        TOKEN = 'token'
        DEVICE_TYPE = 'device'

    TABLE = 'token_pubkey_table'
    COLUMNS = [Column.PUBKEY, Column.TOKEN, Column.DEVICE_TYPE]
    CREATE_TABLE = (
        f'CREATE TABLE IF NOT EXISTS {TABLE} ('
        f'  {Column.PUBKEY} TEXT NOT NULL,'
        f'  {Column.TOKEN} TEXT NOT NULL'
        f')'
    )
    INSERT_DEVICE_TOKEN = (
        f'ALTER TABLE {TABLE}'
        f' ADD {Column.DEVICE_TYPE} TEXT'
    )

    class Token:
        def __init__(self, value, device_type):
            self.value = value
            self.device_type = device_type
            if self.device_type is None:
                self.device_type = DeviceType.iOS if is_ios_device_token(value) else DeviceType.Android

        def __str__(self):
            return self.value

        def __eq__(self, other):
            return self.value == other.value

        def __hash__(self):
            return str(self.value).__hash__()

    def __init__(self, session_id=None):
        self.session_id = session_id
        self.tokens = set()
        self.needs_to_be_updated = False

    def to_database_rows(self):
        rows = []
        for token in self.tokens:
            if isinstance(token, str):
                LokiLogger().logger.error("Token is String.")
                continue
            if isinstance(token.device_type, str):
                LokiLogger().logger.error("Device_Type is String.")
                continue
            rows.append((self.session_id, token.value, token.device_type.value))
        self.needs_to_be_updated = False
        return rows

    def add_token(self, token):
        if isinstance(token, str):
            LokiLogger().logger.error("Adding string token.\n")
            return
        if token not in self.tokens:
            self.tokens.add(token)
            self.needs_to_be_updated = True

    def remove_token(self, token):
        token_type = Device.Token(token, DeviceType.Unknown)
        if token_type in self.tokens:
            self.tokens.remove(token_type)
            self.needs_to_be_updated = True

    def save_to_cache(self, db_helper):
        db_helper.device_cache[self.session_id] = self
        for token in self.tokens:
            db_helper.token_device_mapping[token.value] = self


class ClosedGroup:

    class Column:
        CLOSED_GROUP = 'closedGroupPublicKey'
        PUBKEY = 'pubKey'

    TABLE = 'closed_group_table'
    COLUMNS = [Column.CLOSED_GROUP, Column.PUBKEY]
    CREATE_TABLE = (
        f'CREATE TABLE IF NOT EXISTS {TABLE} ('
        f'  {Column.CLOSED_GROUP} TEXT NOT NULL,'
        f'  {Column.PUBKEY} TEXT NOT NULL'
        f')'
    )

    def __init__(self, closed_group_id=None):
        self.closed_group_id = closed_group_id
        self.members = set()
        self.needs_to_be_updated = False

    def to_database_rows(self):
        rows = []
        for member in self.members:
            rows.append((self.closed_group_id, member))
        self.needs_to_be_updated = False
        return rows

    def add_member(self, member):
        if member not in self.members:
            self.members.add(member)
            self.needs_to_be_updated = True

    def remove_member(self, member):
        if member in self.members:
            self.members.remove(member)
            self.needs_to_be_updated = True

    def save_to_cache(self, db_helper):
        db_helper.closed_group_cache[self.closed_group_id] = self
