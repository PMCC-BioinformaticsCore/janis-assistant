import sqlite3
from contextlib import contextmanager
from os.path import join as ospathjoin

from typing import List, Optional

from janis_assistant.data.providers.rundbprovider import (
    RunDbProvider,
    RunStatusDbProvider,
)

from janis_assistant.data.enums import TaskStatus
from janis_assistant.data.models.run import SubmissionModel, RunModel
from janis_assistant.data.models.workflowjob import RunJobModel
from janis_assistant.data.providers.inputsdbprovider import InputDbProvider
from janis_assistant.data.providers.jobdbprovider import JobDbProvider, groupby
from janis_assistant.data.providers.outputdbprovider import OutputDbProvider
from janis_assistant.data.providers.internalprogressdb import InternalProgressDb
from janis_assistant.data.providers.joblabeldbprovider import JobLabelDbProvider
from janis_assistant.data.providers.submissiondbprovider import SubmissionDbProvider
from janis_assistant.data.providers.workflowmetadataprovider import (
    SubmissionMetadataDbProvider,
)
from janis_assistant.utils.dateutil import DateUtil
from janis_core.utils.logger import Logger


class WorkflowDbManager:
    """
    v0.6.0 refactor.

    The TaskDbManager is split into two major connections to the same database.

    - Sqlite
        - For any row data like: Jobs, Outputs, JobStatuses, etc
        - Regular SQL data
        - Need to define schema, so better for structured data


    - SqliteDict
        - For KV data, like task metadata.
        - Takes care of object serialisation (through Pickle)
        - Stores everything as Blob in database, which means it's unreadable through database viewer
        - Don't need to define schema.

        As this is still regularly expected data, we'll create an object and override the setters / getters
        to get from database. We could even use caching to avoid direct DB hits. We'll try to share this
        object when getting metadata.


    Update 2019-12-23:

        You should be able to respecify the same output directory, which should
        reuse results from your workflow. For this reason, we're going to scope everything by workflow ID.


    Every object here should have a class equivalent that the rest of the program interacts with.
    """

    def __init__(self, submission_id: str, path: str, readonly=False):
        self.exec_path = path
        self.readonly = readonly

        self.submission_id = submission_id

        self.connection = self.db_connection()

        dbkwargs = {
            "db": self.connection,
            "readonly": readonly,
            "submission_id": submission_id,
        }

        self.submissions = SubmissionDbProvider(db=self.connection, readonly=readonly)
        self.runs = RunDbProvider(**dbkwargs)
        self.runevents = RunStatusDbProvider(**dbkwargs)
        self.progressDB = InternalProgressDb(**dbkwargs)
        self.outputsDB = OutputDbProvider(**dbkwargs)
        self.jobsDB = JobDbProvider(**dbkwargs)
        self.inputsDB = InputDbProvider(**dbkwargs)
        self.joblabelsDB = JobLabelDbProvider(**dbkwargs)

        self.submission_metadata = SubmissionMetadataDbProvider(
            db=self.connection,
            readonly=readonly,
            submission_id=submission_id,
            run_id=RunModel.DEFAULT_ID,
        )

    @contextmanager
    def with_cursor(self):
        cursor = None
        try:
            cursor = self.connection.cursor()
            yield cursor
        finally:
            # Change back up
            if cursor:
                cursor.close()

    @staticmethod
    def get_workflow_metadatadb(execpath, wid, readonly=False):

        connection = None
        sqlpath = WorkflowDbManager.get_sql_path_base(execpath)

        Logger.debug("Opening database connection to get wid from: " + sqlpath)
        try:
            connection = sqlite3.connect(f"file:{sqlpath}?mode=ro", uri=True)
        except:
            Logger.critical("Error when opening DB connection to: " + sqlpath)
            raise
        if wid is None:
            wid = SubmissionDbProvider(db=connection, readonly=readonly).get_latest()
        if wid is None:
            raise Exception("Couldn't get WID in task directory")

        retval = SubmissionMetadataDbProvider(
            db=connection,
            readonly=readonly,
            submission_id=wid,
            run_id=RunModel.DEFAULT_ID,
        )
        if connection:
            connection.close()
        return retval

    @staticmethod
    def get_latest_submission_id(path) -> str:
        try:
            connection = sqlite3.connect(
                f"file:{WorkflowDbManager.get_sql_path_base(path)}?mode=ro", uri=True
            )
            submissiondb = SubmissionDbProvider(db=connection, readonly=True)
            return submissiondb.get_latest()

        except:
            Logger.critical("Error when opening DB connection to: " + path)
            raise

    @staticmethod
    def get_sql_path_base(exec_path):
        return ospathjoin(exec_path, "task.db")

    def get_sql_path(self):
        return self.get_sql_path_base(self.exec_path)

    def db_connection(self):
        path = self.get_sql_path()
        try:
            if self.readonly:
                Logger.debug("Opening database connection to in READONLY mode: " + path)
                return sqlite3.connect(f"file:{path}?mode=ro", uri=True)

            Logger.debug("Opening database connection: " + path)
            return sqlite3.connect(path)
        except:
            Logger.critical("Error when opening DB connection to: " + path)
            raise

    def save_metadata(self, metadata: RunModel):

        # mfranklin: DO NOT UPDATE THE STATUS HERE!

        # Let's just say the actual workflow metadata has to updated separately
        self.runs.insert_or_update_many([metadata])
        alljobs = self.flatten_jobs(metadata.jobs or [])
        try:
            self.jobsDB.insert_or_update_many(alljobs)

            self.submission_metadata.metadata.last_updated = DateUtil.now()
            self.submission_metadata.save_changes()

        except Exception as e:
            Logger.warn(f"Error persisting metadata: {repr(e)}")
        # if metadata.error:
        #     self.submission_metadata.error = metadata.error
        # if metadata.execution_dir:
        #     self.submission_metadata.execution_dir = metadata.execution_dir
        #
        # if metadata.finish:
        #     self.submission_metadata.finish = metadata.finish

        return self.commit()

    def get_metadata(self) -> Optional[SubmissionModel]:
        submission = self.submissions.get_by_id(self.submission_id)
        if submission is None:
            Logger.debug("Something happened when getting 'submission' for metadata")
            return

        dbmeta = self.submission_metadata.metadata
        name = dbmeta.name
        status = dbmeta.status
        error = dbmeta.error

        if name:
            submission.name = name
        if status:
            submission.status = status
        if error:
            submission.error = error

        jobs = self.jobsDB.get_all_mapped()
        if jobs is None:
            Logger.log(
                f"Jobs list for {self.submission_metadata.metadata.submission_id} was None, skipping this time."
            )
            return None

        inputs = self.inputsDB.get() or []
        outputs = self.outputsDB.get() or []
        events = self.runevents.get() or []
        joblabels = self.joblabelsDB.get() or []

        events_by_sid = groupby(events, "run_id")
        job_by_sid = groupby(jobs, "run_id")
        inputs_by_sid = groupby(inputs, "run_id")
        outputs_by_sid = groupby(outputs, "run_id")
        joblabels_by_sid = groupby(joblabels, "run_id")
        runs = self.runs.get()

        submission.runs = runs

        self.submission_metadata.update()
        last_updated = self.submission_metadata.metadata.last_updated

        if submission.runs is None:
            return None

        for r in submission.runs:
            r.last_updated = last_updated
            r.jobs = job_by_sid.get(r.id_)
            r.inputs = inputs_by_sid.get(r.id_)
            r.outputs = outputs_by_sid.get(r.id_)
            r.set_events(events_by_sid.get(r.id_))
            if r.jobs is not None and r.id_ in joblabels_by_sid:
                labels_by_jid = {j.job_id: j for j in joblabels_by_sid.get(r.id_)}
                for j in r.jobs:
                    if j.id_ not in labels_by_jid or not j.jobs:
                        continue
                    labels = labels_by_jid[j.id_].labels
                    for child in j.jobs:
                        shard = child.shard
                        if shard is not None and 0 <= shard < len(labels):
                            child.name = labels[shard]

        return submission

        # return WorkflowModel(
        #     wid=self.workflowmetadata.submission_id,
        #     engine_id=self.workflowmetadata.engine_id,
        #     name=self.workflowmetadata.name,
        #     start=self.workflowmetadata.start,
        #     finish=self.workflowmetadata.finish,
        #     outdir=self.exec_path,
        #     execution_dir=self.workflowmetadata.execution_dir,
        #     status=self.workflowmetadata.status,
        #     engine=self.workflowmetadata.engine.description(),
        #     engine_url=self.workflowmetadata.engine_url,
        #     # filesystem=self.workflowmetadata.filesystem.id(),
        #     labels=self.workflowmetadata.labels,
        #     error=self.workflowmetadata.error,
        #     author=self.workflowmetadata.author,
        #     jobs=jobs,
        #     last_updated=self.workflowmetadata.last_updated,
        #     outputs=(
        #         self.outputsDB.get_all()
        #         if self.workflowmetadata.status == TaskStatus.COMPLETED
        #         else None
        #     ),
        #     inputs=inputs,
        # )

    @staticmethod
    def flatten_jobs(jobs: List[RunJobModel]):
        flattened = []
        for j in jobs:
            flattened.append(j)
            if j.jobs:
                flattened.extend(WorkflowDbManager.flatten_jobs(j.jobs))
        return flattened

    def commit(self):
        if self.connection:
            self.connection.commit()
        else:
            Logger.critical("Couldn't commit to DB connection")

    def close(self):
        self.connection.close()
        self.connection = None
