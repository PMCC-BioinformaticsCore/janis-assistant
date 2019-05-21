import sqlite3
from typing import Tuple, List

from shepherd.management.enums import InfoKeys, ProgressKeys
from shepherd.utils.logger import Logger


class DatabaseManager:

    def __init__(self, tid, path):
        self.tid = tid
        self.exec_path = path
        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.create_info_table_if_required()
        self.create_progress_table_if_required()

    def get_sql_path(self):
        return self.exec_path + "janis.db"

    def db_connection(self):
        path = self.get_sql_path()
        Logger.log("Opening database connection to: " + path)
        return sqlite3.connect(path)

    def commit(self):
        return self.connection.commit()

    ## TABLE creaters

    def create_info_table_if_required(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS info 
                                (key text PRIMARY KEY, value text)""")

    def create_progress_table_if_required(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS progress
                                (key text PRIMARY KEY)""")

    ## getters and setters

    # info

    def add_meta_info(self, key: InfoKeys, value: any):
        self.add_meta_infos([(key, value)])

    def add_meta_infos(self, infos: List[Tuple[InfoKeys, any]]):
        for key, value in infos:
            if not isinstance(value, list):
                value = [value]
            for v in value:
                self.cursor.execute("INSERT INTO info VALUES (?, ?)", (str(key), str(v)))
        self.commit()

    def get_meta_info(self, key: InfoKeys):
        self.cursor.execute("SELECT value from info where key = ?", (str(key), ))
        return self.cursor.fetchone()[0]

    def get_engine_identifier(self):
        return self.get_meta_info(InfoKeys.engineId)

    # progress

    def progress_mark_completed(self, key: ProgressKeys):
        return self.cursor.execute("INSERT INTO progress VALUES (?)", (str(key), ))

    def progress_has_completed(self, key: ProgressKeys):
        return self.cursor.execute("SELECT count(*) from progress where key = ?", (str(key), )).fetchone()[0] > 0
