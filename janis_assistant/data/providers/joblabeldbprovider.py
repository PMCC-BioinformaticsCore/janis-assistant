from sqlite3 import Connection

from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.models.joblabel import JobLabelModel


class JobLabelDbProvider(DbProviderBase[JobLabelModel]):
    CURRENT_SCHEMA_VERSION = 1

    def __init__(self, db: Connection, submission_id: str):
        super().__init__(
            base_type=JobLabelModel,
            db=db,
            tablename="joblabels",
            scopes={"submission_id": submission_id},
        )
        self.submission_id = submission_id

    def get_by_run_id(self, run_id):
        return self.get(where=("run_id = ?", [run_id]))
