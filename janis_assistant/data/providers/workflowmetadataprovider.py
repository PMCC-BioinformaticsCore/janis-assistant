from datetime import datetime

from typing import Optional

from janis_assistant.data.models.run import SubmissionModel, RunModel

from janis_assistant.data.enums.workflowmetadatakeys import WorkflowMetadataDbKeys
from janis_assistant.data.keyvaluedbproviderbase import KvDB
from janis_assistant.management.filescheme import FileScheme
from janis_assistant.engines import Engine
from janis_assistant.utils.getuser import lookup_username


class SubmissionMetadataDbProvider(KvDB):

    attributes_to_persist = {a.value for a in WorkflowMetadataDbKeys}

    @staticmethod
    def new(dblocation: str, submission_id: str):
        t = SubmissionMetadataDbProvider(dblocation, submission_id)

        t.submission_id = submission_id
        t.start = datetime.now()

        return t

    def __init__(self, dblocation, submission_id, readonly=False):
        super().__init__(dblocation, "workflow-" + submission_id, readonly=readonly)
        self.kvdb.autocommit = False
        if WorkflowMetadataDbKeys.submission_id.value not in self.kvdb:
            # Initialise to give prompts to IDE
            self.submission_id = None
            self.name = None

            self.status = None
            self.last_updated = None
            self.please_abort = False
            self.please_pause = False

            self.keepexecutiondir = None

            self.containerversion = None
            self.containertype = None

            self.engine: Optional[Engine] = None
            self.filescheme: Optional[FileScheme] = None

            self.configuration = None
            self.dbconfig = None

        self.kvdb.autocommit = True
        self.kvdb.commit()

    def to_model(self):
        return RunModel(
            id_=RunModel.DEFAULT_ID,
            submission_id=self.submission_id,
            engine_id=self.engine_wid,
            name=self.name,
            status=self.status,
            # start=self.start,
            # finish=self.finish,
            execution_dir=self.execution_dir,
            # en=self.engine.id() if self.engine else None,
            # filesystem=self.filescheme.id() if self.filescheme else None,
            labels=self.labels,
            error=self.error,
            # author=self.author,
        )
