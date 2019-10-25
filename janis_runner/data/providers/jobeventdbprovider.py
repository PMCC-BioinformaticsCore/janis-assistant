from typing import List

from janis_runner.data.dbproviderbase import DbProviderBase
from janis_runner.data.models.workflowjob import WorkflowJobEventModel


class JobEventDbProvider(DbProviderBase):
    CURRENT_SCHEMA_VERSION = 1

    def table_schema(self):
        return """\
        CREATE TABLE IF NOT EXISTS jobevents (
            jid STRING NOT NULL,
            status STRING NOT NULL,
            timestamp STRING NOT NULL,
            PRIMARY KEY (jid, status),
            FOREIGN KEY (jid) REFERENCES jobs (jid)
        )
        """

    def __init__(self, db, cursor):
        super().__init__(db, cursor)

    def get(self, jid: str) -> List[WorkflowJobEventModel]:
        self.cursor.execute("SELECT * FROM jobevents WHERE jid = ?", (jid,))
        rows = self.cursor.fetchall()
        if not rows:
            return []

        return [WorkflowJobEventModel.from_row(r) for r in rows]

    def get_all(self) -> List[WorkflowJobEventModel]:
        self.cursor.execute("SELECT * FROM jobevents")
        rows = self.cursor.fetchall()
        return [WorkflowJobEventModel.from_row(row) for row in rows]

    def insert(self, model: WorkflowJobEventModel):
        self.cursor.execute(self._insert_statement, self._insert_model_obj(model))

    def _insert_model_obj(self, model: WorkflowJobEventModel):
        return (model.jid, model.status, model.timestamp)

    _insert_statement = """\
        INSERT INTO jobevents
            (jid, status, timestamp)
        VALUES
            (?, ?, ?)
        """

    def insert_many(self, events: List[WorkflowJobEventModel]):
        pks = self.cursor.execute("SELECT jid, status FROM jobevents").fetchall()
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
