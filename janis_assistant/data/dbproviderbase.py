from abc import abstractmethod
from sqlite3 import Connection
from contextlib import contextmanager


class DbProviderBase:
    def __init__(self, db: Connection):
        self.db: Connection = db

        schema = self.table_schema()
        with self.with_cursor() as cursor:
            cursor.execute(schema)

    @contextmanager
    def with_cursor(self):
        cursor = None
        try:
            cursor = self.db.cursor()
            yield cursor
        finally:
            # Change back up
            if cursor:
                cursor.close()

    def commit(self):
        return self.db.commit()

    @abstractmethod
    def table_schema(self):
        pass
