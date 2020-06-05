from typing import List, Optional
from sqlite3 import OperationalError

from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.models.workflowjob import WorkflowJobEventModel
from janis_core.utils.logger import Logger


class JobEventDbProvider(DbProviderBase):
    CURRENT_SCHEMA_VERSION = 1

    def table_schema(self):
        return """\
        CREATE TABLE IF NOT EXISTS jobevents (
            wid STRING NOT NULL,
            jid STRING NOT NULL,
            status STRING NOT NULL,
            timestamp STRING NOT NULL,
            PRIMARY KEY (wid, jid, status),
            FOREIGN KEY (jid) REFERENCES jobs (jid)
        )
        """

    def __init__(self, db, wid):
        super().__init__(db)
        self.wid = wid

    def get(self, jid: str) -> List[WorkflowJobEventModel]:
        with self.with_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM jobevents WHERE wid = ? AND jid = ?", (self.wid, jid)
            )
            rows = cursor.fetchall()
        if not rows:
            return []

        return [WorkflowJobEventModel.from_row(r) for r in rows]

    def get_all(self) -> Optional[List[WorkflowJobEventModel]]:
        query = "SELECT * FROM jobevents WHERE wid = ?"

        with self.with_cursor() as cursor:
            try:
                cursor.execute(query, (self.wid,))
                rows = cursor.fetchall()
            except OperationalError as e:
                if "readonly database" in str(e):
                    # mfranklin: idk, this sometimes happens. We're doing a select query, idk sqlite3 driver...
                    Logger.debug(
                        "Got readonly error when running query: '{query}', skipping for now"
                    )
                    return None
                elif "locked" in str(e):
                    Logger.debug(
                        "We hit the database at the same time the janis process wrote to it, meh"
                    )
                    return None
                raise e

        return [WorkflowJobEventModel.from_row(row) for row in rows]

    def insert(self, model: WorkflowJobEventModel):
        with self.with_cursor() as cursor:

            cursor.execute(
                self._insert_statement, (self.wid, *self._insert_model_obj(model))
            )

    def _insert_model_obj(self, model: WorkflowJobEventModel):
        return (self.wid, model.jid, model.status, model.timestamp)

    _insert_statement = """\
        INSERT INTO jobevents
            (wid, jid, status, timestamp)
        VALUES
            (?, ?, ?, ?)
        """

    def insert_many(self, events: List[WorkflowJobEventModel]):
        with self.with_cursor() as cursor:
            pks = cursor.execute(
                "SELECT jid, status FROM jobevents WHERE wid = ?", (self.wid,)
            ).fetchall()
            pkset = {f"{jid}-{status}" for (jid, status) in pks}

            inserts = []

            for e in events:
                pk = f"{e.jid}-{e.status}"
                if pk not in pkset:
                    inserts.append(self._insert_model_obj(e))
            results = None
            if len(inserts) > 0:
                results = cursor.executemany(self._insert_statement, inserts)

        return results

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
