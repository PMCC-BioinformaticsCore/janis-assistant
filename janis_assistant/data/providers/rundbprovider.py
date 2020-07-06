from datetime import datetime
from typing import List, Optional, Dict, Set

from janis_assistant.data.models.run import RunModel, SubmissionModel
from janis_assistant.utils.dateutil import DateUtil

from janis_assistant.data.dbproviderbase import DbProviderBase


class RunDbProvider(DbProviderBase[RunModel]):
    def __init__(self, db, submission_id: str):
        super().__init__(
            base_type=RunModel,
            db=db,
            tablename="runs",
            scopes={"submission_id": submission_id},
        )
