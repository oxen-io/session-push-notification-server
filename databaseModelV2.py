class Device:
    def __init__(self, session_id=None):
        self.session_id = session_id
        self.tokens = set()
        self.needs_to_be_updated = False

    def to_database_rows(self):
        rows = []
        for token in self.tokens:
            rows.append((self.session_id, token))
        return rows

    def add_token(self, token):
        if token not in self.tokens:
            self.tokens.add(token)
            self.needs_to_be_updated = True

    def remove_token(self, token):
        if token in self.tokens:
            self.tokens.remove(token)
            self.needs_to_be_updated = True

    def save_to_cache(self, db_helper):
        db_helper.device_cache[self.session_id] = self
        for token in self.tokens:
            db_helper.token_device_mapping[token] = self


class ClosedGroup:
    def __init__(self, closed_group_id=None):
        self.closed_group_id = closed_group_id
        self.members = set()
        self.needs_to_be_updated = False

    def to_database_rows(self):
        rows = []
        for member in self.members:
            rows.append((self.closed_group_id, member))
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