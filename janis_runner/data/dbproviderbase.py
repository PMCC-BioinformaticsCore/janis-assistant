from sqlite3 import Connection, Cursor


class DbProviderBase:
    def __init__(self, db, cursor):
        self.db: Connection = db
        self.cursor: Cursor = cursor

    def commit(self):
        return self.db.commit()
