from datetime import datetime
from typing import List, Optional, Dict

from janis_assistant.data.models.run import RunModel, SubmissionModel
from janis_assistant.utils.dateutil import DateUtil

from janis_assistant.data.dbproviderbase import DbProviderBase


class SubmissionDbProvider(DbProviderBase[SubmissionModel]):
    CURRENT_SCHEMA_VERSION = 2

    def __init__(self, db):
        super().__init__(
            base_type=SubmissionModel, db=db, tablename="submissions", scopes={}
        )

    def get_latest(self) -> str:
        with self.with_cursor() as cursor:
            cursor.execute(
                f"SELECT id FROM {self.tablename} ORDER BY timestamp DESC LIMIT 1"
            )
            latest = cursor.fetchone()[0]

        return latest

    def get_single(self, id_: str) -> Optional[SubmissionModel]:
        rows = self.get(where=("id = ?", [id_]))
        if not rows:
            return None
        return rows[0]

    def get_all(self) -> List[SubmissionModel]:
        return self.get()