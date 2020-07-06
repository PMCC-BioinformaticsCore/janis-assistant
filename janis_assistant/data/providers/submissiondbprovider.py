from datetime import datetime
from typing import List, Optional, Dict, Set

from janis_assistant.data.models.run import RunModel, SubmissionModel
from janis_assistant.utils.dateutil import DateUtil

from janis_assistant.data.dbproviderbase import DbProviderBase


class SubmissionDbProvider(DbProviderBase[SubmissionModel]):
    CURRENT_SCHEMA_VERSION = 2

    def __init__(self, db):
        super().__init__(
            base_type=SubmissionModel, db=db, tablename="submissions", scopes={}
        )

    def get_by_id(self, submission_id) -> Optional[SubmissionModel]:
        s = self.get(where=("id = ?", [submission_id]))
        if len(s) != 1:
            return None
        return s[0]

    def get_existing_ids(self) -> Set[str]:
        query = f"SELECT id FROM {self.tablename} ORDER BY timestamp"
        with self.with_cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        if not rows:
            return set()

        return set(r[0] for r in rows)

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

    def insert(self, submission_id):
        return self.insert_or_update_many([SubmissionModel(id_=submission_id)])
