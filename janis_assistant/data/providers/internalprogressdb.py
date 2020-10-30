from datetime import datetime
from typing import Dict

from janis_assistant.utils.dateutils import DateUtil

from janis_assistant.data.dbproviderbase import DbProviderBase

from janis_assistant.data.enums import ProgressKeys
from janis_assistant.data.keyvaluedbproviderbase import KvDB

# class InternalProgressDb(KvDB):
#     attributes_to_persist = [k.value for k in ProgressKeys]
#
#     def __init__(self, dblocation):
#         super().__init__(dblocation, tablename="progress")
#
#         if "start" not in self.kvdb:
#             for k in self.attributes_to_persist:
#                 self.__setattr__(k, False)
#
#             self.start = True
#             self.createOutputStructure = False
#             self.saveWorkflow = False
#             self.submitWorkflow = False
#             self.workflowMovedToFinalState = False
#             self.copiedOutputs = False
#             self.savedMetadata = False
#             self.savedLogs = False
#             self.cleanedUp = False


class InternalProgressDb(DbProviderBase):
    CURRENT_SCHEMA_VERSION = 1

    def table_schema(self):
        return """\
        CREATE TABLE IF NOT EXISTS progress (
            submission_id STRING NOT NULL,
            key STRING NOT NULL,
            timestamp STRING NOT NULL,
            PRIMARY KEY (submission_id, key)
        )
        """

    def __init__(self, db, readonly, submission_id):
        super().__init__(
            None,
            db,
            readonly=readonly,
            tablename="progress",
            scopes={"submission_id": submission_id},
        )
        self.submission_id = submission_id

    def has(self, key: ProgressKeys):
        with self.with_cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM progress WHERE submission_id = ? AND key = ?",
                (self.submission_id, key.value),
            )
            rows = cursor.fetchone()
            return bool(rows)

    def get_all(self) -> Dict[str, datetime]:
        with self.with_cursor() as cursor:
            cursor.execute(
                "SELECT key, timestamp FROM progress WHERE submission_id = ?",
                (self.submission_id,),
            )
            rows = cursor.fetchall()
            return {row[0]: DateUtil.parse_iso(row[1]) for row in rows}

    def set(self, key: ProgressKeys):
        if self.has(key):
            return

        with self.with_cursor() as cursor:
            cursor.execute(
                self._insert_statement,
                (self.submission_id, key.value, str(DateUtil.now())),
            )

    _insert_statement = """\
        INSERT INTO progress
            (submission_id, key, timestamp)
        VALUES
            (?, ?, ?)
        """

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
