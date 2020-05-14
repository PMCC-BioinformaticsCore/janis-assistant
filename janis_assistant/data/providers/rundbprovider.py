from datetime import datetime
from typing import List, Optional, Dict

from janis_assistant.utils.dateutil import DateUtil

from janis_assistant.data.dbproviderbase import DbProviderBase


class RunDbProvider(DbProviderBase):
    CURRENT_SCHEMA_VERSION = 1

    def table_schema(self):
        return """\
        CREATE TABLE IF NOT EXISTS runs (
            wid STRING PRIMARY KEY,
            timestamp STRING
        )
        """

    def __init__(self, db):
        super().__init__(db)

    def get_latest(self):
        with self.with_cursor() as cursor:
            cursor.execute("SELECT wid FROM runs ORDER BY timestamp DESC LIMIT 1")
            latest = cursor.fetchone()[0]

        return latest

    def get(self, wid: str) -> Optional[datetime]:
        with self.with_cursor() as cursor:
            cursor.execute("SELECT timestamp FROM runs WHERE wid = ?", (wid,))
            row = cursor.fetchone()

        if not row:
            return None

        return DateUtil.parse_iso(row[0])

    def get_all(self) -> Dict[str, datetime]:
        with self.with_cursor() as cursor:
            cursor.execute("SELECT wid, timestamp FROM runs")
            rows = cursor.fetchall()

        return {row[0]: DateUtil.parse_iso(row[1]) for row in rows}

    def insert(self, wid: str):
        with self.with_cursor() as cursor:

            cursor.execute(self._insert_statement, (wid, str(DateUtil.now())))

    _insert_statement = """\
        INSERT INTO runs
            (wid, timestamp)
        VALUES
            (?, ?)
        """

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
