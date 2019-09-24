import sqlite3
from typing import Tuple, List

from janis_runner.data.enums import InfoKeys, ProgressKeys
from janis_runner.data.models.filescheme import FileScheme
from janis_runner.data.providers.config.enginedbprovider import EngineDbProvider
from janis_runner.data.providers.config.fileschemedbprovider import FileschemeDbProvider
from janis_runner.engines import Engine
from janis_runner.utils.logger import Logger


class TaskDbManager:
    """
    v0.6.0 refactor.

    The TaskDbManager is split into two major connections to the same database.

    - Sqlite
        - For any row data like: Jobs, Outputs, JobStatuses, etc
        - Regular SQL data
        - Need to define schema, so better for regular occuring data


    - SqliteDict
        - For KV data, like task metadata.
        - Takes care of object serialisation (through Pickle)
        - Stores everything as Blob in database, which means it's unreadable through database viewer
        - Don't need to define schema.

        As this is still regularly expected data, we'll create an object and override the setters / getters
        to get from database. We could even use caching to avoid direct DB hits. We'll try to share this
        object when getting metadata.


    Every object here should have a class equivalent that the rest of the program interacts with.

    """

    def __init__(self, path):
        self.exec_path = path
        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.create_info_table_if_required()
        self.create_progress_table_if_required()
        self.engineDB = EngineDbProvider(self.connection, self.cursor)
        self.fileschemeDB = FileschemeDbProvider(self.connection, self.cursor)

    def get_sql_path(self):
        return self.exec_path + "task.db"

    def db_connection(self):
        path = self.get_sql_path()
        Logger.log("Opening database connection to: " + path)
        return sqlite3.connect(path)

    def commit(self):
        return self.connection.commit()

    # TABLE creators

    def create_info_table_if_required(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS info 
                                (key text PRIMARY KEY, value text)"""
        )

    def create_progress_table_if_required(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS progress
                                (key text PRIMARY KEY)"""
        )

    # getters and setters

    # info

    def add_meta_info(self, key: InfoKeys, value: any):
        self.add_meta_infos([(key, value)])

    def add_meta_infos(self, infos: List[Tuple[InfoKeys, any]]):
        for key, value in infos:
            if not isinstance(value, list):
                value = [value]
            for v in value:
                self.cursor.execute(
                    "INSERT INTO info VALUES (?, ?)", (str(key), str(v))
                )
        self.commit()

    def update_meta_info(self, key: InfoKeys, value: any):
        return self.update_meta_infos([(key, value)])

    def update_meta_infos(self, infos: List[Tuple[InfoKeys, any]]):
        for key, value in infos:
            if not isinstance(value, list):
                value = [value]
            for v in value:
                self.cursor.execute(
                    "UPDATE info SET value = ? WHERE key = ?", (str(v), str(key))
                )
        self.commit()

    def get_meta_info(self, key: InfoKeys):
        self.cursor.execute("SELECT value FROM info WHERE key = ?", (str(key),))
        only_one_row = self.cursor.fetchone()
        if not only_one_row:
            return None
        return only_one_row[0]

    def get_all_meta_info(self):
        results = self.cursor.execute("SELECT key, value FROM info").fetchall()
        return {v[0]: v[1] for v in results}

    def get_engine_identifier(self):
        return self.get_meta_info(InfoKeys.engineId)

    # progress

    def progress_mark_completed(self, key: ProgressKeys):
        self.cursor.execute("INSERT INTO progress VALUES (?)", (str(key),))
        self.commit()

    def progress_has_completed(self, key: ProgressKeys):
        return (
            self.cursor.execute(
                "SELECT count(*) from progress where key = ?", (str(key),)
            ).fetchone()[0]
            > 0
        )

    def close(self):
        self.connection.close()
        self.cursor = None
        self.connection = None

    # engines

    def persist_engine(self, engine: Engine):
        return self.engineDB.persist(engine)

    def get_engine(self):
        engid = self.get_meta_info(InfoKeys.engineId)
        return self.engineDB.get(engid)

    # fileschemes

    def persist_filescheme(self, filescheme: FileScheme):
        return self.fileschemeDB.persist(filescheme)

    def get_filescheme(self):
        fsid = self.get_meta_info(InfoKeys.fileschemeId)
        return self.fileschemeDB.get(fsid)
