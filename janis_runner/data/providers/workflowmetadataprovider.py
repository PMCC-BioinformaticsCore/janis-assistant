from datetime import datetime

from janis_runner.data.enums.workflowmetadatakeys import WorkflowMetadataDbKeys
from janis_runner.data.keyvaluedbproviderbase import KvDB
from janis_runner.data.models.filescheme import FileScheme
from janis_runner.data.models.workflow import WorkflowModel
from janis_runner.engines import Engine
from janis_runner.utils.getuser import lookup_username


class WorkflowMetadataDbProvider(KvDB):

    attributes_to_persist = {a.value for a in WorkflowMetadataDbKeys}

    @staticmethod
    def new(dblocation: str, wid: str):
        t = WorkflowMetadataDbProvider(dblocation)

        t.wid = wid
        t.start = datetime.now()

        return t

    def __init__(self, dblocation):
        super().__init__(dblocation, "workflow")
        self.kvdb.autocommit = False
        if WorkflowMetadataDbKeys.wid.value not in self.kvdb:
            # Initialise to give prompts to IDE
            self.wid = None
            self.engine_wid = None

            self.name = None

            self.status = None
            self.start = None
            self.finish = None
            self.last_updated = None
            self.please_abort = False

            self.execution_dir = None
            self.keepexecutiondir = None

            self.engine: Engine = None
            self.filescheme: FileScheme = None
            self.labels = None
            self.error = None

            self.configuration = None
            self.manages_database = None
            self.should_manage_database = True

            self.author = lookup_username()

        self.kvdb.autocommit = True
        self.kvdb.commit()

    def to_model(self):
        return WorkflowModel(
            wid=self.wid,
            engine_wid=self.engine_wid,
            name=self.name,
            status=self.status,
            start=self.start,
            finish=self.finish,
            execution_dir=self.execution_dir,
            engine=self.engine.id() if self.engine else None,
            filesystem=self.filesystem.id() if self.filesystem else None,
            labels=self.labels,
            error=self.error,
            author=self.author,
        )
