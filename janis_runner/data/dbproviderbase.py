from abc import abstractmethod
from sqlite3 import Connection, Cursor


class DbProviderBase:
    def __init__(self, db, cursor):
        self.db: Connection = db
        self.cursor: Cursor = cursor

        schema = self.table_schema()
        self.cursor.execute(schema)

    def commit(self):
        return self.db.commit()

    @abstractmethod
    def table_schema(self):
        pass
