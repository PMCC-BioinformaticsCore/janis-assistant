import os
import sqlite3
from datetime import datetime
from enum import Enum

from shepherd.data.schema import TaskStatus


class DatabaseManager:

    class InfoKeys(Enum):
        engineId = "engineId"
        status = "status"
        start = "start"
        validating = "validating"

        def __str__(self):
            return self.value

    def __init__(self, tid, path):
        self.tid = tid
        self.exec_path = path
        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

    def get_sql_path(self):
        return self.exec_path + "janis.db"

    def db_connection(self):
        path = self.get_sql_path()
        return sqlite3.connect(path)

    def commit(self):
        return self.connection.commit()

    def table_exists(self, tablename):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (tablename, ))
        row = self.cursor.fetchone()
        return row is not None

    def create_info_table_if_required(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS info 
                                (key text PRIMARY KEY, value text)""")

    def add_meta_info(self, key: InfoKeys, value: any):
        self.cursor.execute("INSERT INTO info VALUES (?, ?)", (str(key), str(value)))
        self.commit()

    def get_engine_identifier(self):
        self.cursor.execute("SELECT value from info where key = ?", (str(DatabaseManager.InfoKeys.engineId), ))
        row = self.cursor.fetchone()
        return row[0]


# if __name__ == "__main__":
#     con = DatabaseManager("test-1")
#     c = con.cursor()
#     # c.execute('''CREATE TABLE stocks
#     #          (date text, trans text, symbol text, qty real, price real)''')
#     c.execute(''' INSERT INTO stocks VALUES ('2019', 'BUY', 'product', 1, 9.99)''')
#     c.execute('SELECT * FROM stocks')
#     print(c.fetchall())
#     con.commit()