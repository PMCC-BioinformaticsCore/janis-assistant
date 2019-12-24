from typing import List

from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.models.workflowjob import WorkflowJobEventModel


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

    def __init__(self, db, cursor, wid):
        super().__init__(db, cursor)
        self.wid = wid

    def get(self, jid: str) -> List[WorkflowJobEventModel]:
        self.cursor.execute(
            "SELECT * FROM jobevents WHERE wid = ? AND jid = ?", (self.wid, jid)
        )
        rows = self.cursor.fetchall()
        if not rows:
            return []

        return [WorkflowJobEventModel.from_row(r) for r in rows]

    def get_all(self) -> List[WorkflowJobEventModel]:
        self.cursor.execute("SELECT * FROM jobevents WHERE wid = ?", (self.wid,))
        rows = self.cursor.fetchall()
        return [WorkflowJobEventModel.from_row(row) for row in rows]

    def insert(self, model: WorkflowJobEventModel):
        self.cursor.execute(
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
        pks = self.cursor.execute(
            "SELECT jid, status FROM jobevents WHERE wid = ?", (self.wid,)
        ).fetchall()
        pkset = {f"{jid}-{status}" for (jid, status) in pks}

        inserts = []

        for e in events:
            pk = f"{e.jid}-{e.status}"
            if pk not in pkset:
                inserts.append(self._insert_model_obj(e))

        if len(inserts) > 0:
            return self.cursor.executemany(self._insert_statement, inserts)

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
