from datetime import datetime
from typing import List, Optional, Dict, Set

from janis_assistant.data.enums import TaskStatus

from janis_assistant.data.models.run import RunModel, SubmissionModel, RunStatusUpdate
from janis_assistant.utils.dateutil import DateUtil

from janis_assistant.data.dbproviderbase import DbProviderBase


class RunDbProvider(DbProviderBase[RunModel]):
    def __init__(self, db, readonly, submission_id: str):
        super().__init__(
            base_type=RunModel,
            db=db,
            readonly=readonly,
            tablename="runs",
            scopes={"submission_id": submission_id},
        )

    def insert_or_update_many(self, els: List[RunModel]):
        for el in els:
            el.last_updated = DateUtil.now()
        return super().insert_or_update_many(els)


class RunStatusDbProvider(DbProviderBase[RunStatusUpdate]):
    def __init__(self, db, readonly, submission_id: str):
        super().__init__(
            base_type=RunStatusUpdate,
            db=db,
            readonly=readonly,
            tablename="runstatus",
            scopes={"submission_id": submission_id},
        )

        self.submission_id = submission_id

    def update(self, run_id: str, status: TaskStatus):
        return self.insert_or_update_many(
            [
                RunStatusUpdate(
                    submission_id=self.submission_id,
                    run_id=run_id,
                    status=status,
                    date=DateUtil.now(),
                )
            ]
        )
