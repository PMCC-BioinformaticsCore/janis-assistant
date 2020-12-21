from typing import Optional, Set

from janis_core import Logger

from janis_assistant.data.enums import TaskStatus
from janis_assistant.data.keyvaluedbproviderbase import KvDB
from janis_assistant.data.models.base import KVDatabaseObject, unpickle_obj
from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.management.configuration import (
    DatabaseTypeToUse,
    JanisDatabaseConfigurationHelper,
)


class SubmissionDbMetadata(KVDatabaseObject):
    def __init__(
        self,
        submission_id: str = None,
        run_id: str = None,
        prepared_job: PreparedJob = None,
        name: str = None,
        status: TaskStatus = None,
        last_updated=None,
        keep_execution_dir=True,
        container_version=None,
        container_type=None,
        engine=None,
        engine_id: str = None,
        submission_workflow=None,
        submission_inputs=None,
        submission_resources=None,
        error=None,
        db_configuration: JanisDatabaseConfigurationHelper = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._submission_id = submission_id
        self._run_id = run_id
        self.status = TaskStatus(status) if status is not None else None

        self.prepared_job = prepared_job

        self.last_updated = last_updated
        self.keep_execution_dir = keep_execution_dir
        self.container_version = container_version
        self.container_type = container_type
        self.engine = engine
        self.engine_id = engine_id
        self.db_configuration = db_configuration
        self.name = name
        self.error = error

        self.submission_workflow = submission_workflow
        self.submission_inputs = submission_inputs
        self.submission_resources = submission_resources

    @classmethod
    def fields_to_encode(cls) -> Optional[Set[str]]:
        return {
            "engine",
            "configuration",
            "container_type",
            "db_type",
            "start",
            "status",
        }


class SubmissionMetadataDbProvider(KvDB):
    def __init__(
        self,
        db,
        readonly: bool,
        submission_id: str,
        run_id: str,
        metadata: Optional[SubmissionDbMetadata] = None,
    ):
        super().__init__(
            db=db,
            readonly=readonly,
            tablename="submission_metadata",
            scopes={"submission_id": submission_id, "run_id": run_id},
        )

        self.metadata = metadata if metadata is not None else self.get()

    def get_uncached_status(self) -> TaskStatus:
        scopes_dict = {**self._scopes, "id": "status"}
        scope_keys = list(scopes_dict.keys())
        scopes = " AND ".join(f"{k} = ?" for k in scope_keys)
        scope_values = [scopes_dict[k] for k in scope_keys]
        query = f"SELECT value FROM {self._tablename} WHERE {scopes}"
        with self.with_cursor() as cursor:
            row = cursor.execute(query, scope_values).fetchone()
            return unpickle_obj(row[0])

    def set_metadata(self, obj: SubmissionDbMetadata):
        self.metadata = obj
        self.commit()

    def update(self):
        self.metadata.update_from_rows(self.get_rows())
        self.metadata.discard_changes()

    def get(self) -> SubmissionDbMetadata:
        return SubmissionDbMetadata.decode_rows_to_dict(self.get_rows())

    def save(self):
        self.save_encoded_rows(self.metadata.get_encoded_rows())

    def save_changes(self):
        rows = self.metadata.get_encoded_changes()
        Logger.log(f"Updating workflow fields: {rows}")
        self.save_encoded_rows(rows)
        self.metadata.discard_changes()


# class SubmissionMetadataDbProvider(KvDB):
#
#     attributes_to_persist = {a.value for a in WorkflowMetadataDbKeys}
#
#     @staticmethod
#     def new(dblocation: str, submission_id: str):
#         t = SubmissionMetadataDbProvider(dblocation, submission_id)
#
#         t.submission_id = submission_id
#         t.start = datetime.now()
#
#         return t
#
#     def __init__(self, dblocation, submission_id, readonly=False):
#         super().__init__(dblocation, "workflow-" + submission_id, readonly=readonly)
#         self.kvdb.autocommit = False
#         if WorkflowMetadataDbKeys.submission_id.value not in self.kvdb:
#             # Initialise to give prompts to IDE
#             self.submission_id = None
#             self.name = None
#
#             self.status = None
#             self.last_updated = None
#             self.please_abort = False
#             self.please_pause = False
#
#             self.keepexecutiondir = None
#
#             self.containerversion = None
#             self.containertype = None
#
#             self.engine: Optional[Engine] = None
#             self.filescheme: Optional[FileScheme] = None
#
#             self.configuration = None
#             self.dbconfig = None
#
#         self.kvdb.autocommit = True
#         self.kvdb.commit()
#
#     def to_model(self):
#         return RunModel(
#             id_=RunModel.DEFAULT_ID,
#             submission_id=self.submission_id,
#             engine_id=self.engine_id,
#             name=self.name,
#             status=self.status,
#             # start=self.start,
#             # finish=self.finish,
#             execution_dir=self.execution_dir,
#             # en=self.engine.id() if self.engine else None,
#             # filesystem=self.filescheme.id() if self.filescheme else None,
#             labels=self.labels,
#             error=self.error,
#             # author=self.author,
#         )
