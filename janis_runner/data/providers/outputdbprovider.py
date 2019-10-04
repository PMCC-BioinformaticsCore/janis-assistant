from typing import List

from janis_runner.data.dbproviderbase import DbProviderBase
from janis_runner.data.models.outputs import WorkflowOutputModel


class OutputDbProvider(DbProviderBase):
    CURRENT_SCHEMA_VERSION = 1

    def table_schema(self):
        return """\
        CREATE TABLE IF NOT EXISTS outputs (
            tag STRING PRIMARY KEY,
            original_path STRING,
            new_path STRING,
            prefix STRING
            tags STRING
            timestamp NULLABLE STRING
        )
        """

    def __init__(self, db, cursor):
        super().__init__(db, cursor)

    def get(self, tag: str) -> WorkflowOutputModel:
        self.cursor.execute("SELECT * FROM outputs WHERE tag = ?", (tag,))
        row = self.cursor.fetchone()
        if not row:
            raise KeyError("Couldn't find output with tag: " + tag)
        return WorkflowOutputModel.from_row(row)

    def get_all(self, tag: str) -> List[WorkflowOutputModel]:
        self.cursor.execute("SELECT * FROM outputs", (tag,))
        rows = self.cursor.fetchall()
        return [WorkflowOutputModel.from_row(row) for row in rows]

    def insert(self, model: WorkflowOutputModel):
        self.cursor.execute(
            """\
            INSERT INTO outputs
                (tag, original_path, new_path, prefix, tags, timestamp)
            VALUES
                (?, ?, ?, ?, ?)
            """,
            (
                model.tag,
                model.originalpath,
                model.newpath,
                model.prefix,
                model.tags,
                model.timestamp,
            ),
        )

    def update(self, model: WorkflowOutputModel):
        self.cursor.execute(
            """\
            UPDATE outputs SET
                original_path=?,
                new_path=?,
                prefix=?,
                tags=?,
                timestamp=?
            WHERE tag = ?
            """,
            (
                model.originalpath,
                model.newpath,
                model.prefix,
                model.tags,
                model.timestamp,
                model.tag,
            ),
        )

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
