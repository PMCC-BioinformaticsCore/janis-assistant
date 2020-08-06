import json
from datetime import datetime
from typing import List

from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.utils.dateutil import DateUtil


class OutputDbProvider(DbProviderBase[WorkflowOutputModel]):
    CURRENT_SCHEMA_VERSION = 1

    def __init__(self, db, readonly, submission_id):
        super().__init__(
            base_type=WorkflowOutputModel,
            db=db,
            readonly=readonly,
            tablename="outputs",
            scopes={"submission_id": submission_id},
        )
        self.submission_id = submission_id

    def insert_many(self, outputs: List[WorkflowOutputModel]):
        return self.insert_or_update_many(outputs)

    def update_paths(
        self, run_id: str, tag: str, original_path: str, new_path: str, value: any
    ):
        model = WorkflowOutputModel(
            id_=tag,
            submission_id=self.submission_id,
            run_id=run_id,
            original_path=original_path,
            new_path=new_path,
            timestamp=datetime.now(),
            value=value,
            # empty fields
            extension=None,
            is_copyable=None,
            output_folder=None,
            output_name=None,
            secondaries=None,
        )
        self.insert_or_update_many([model])

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
