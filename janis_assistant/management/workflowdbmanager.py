import sqlite3
from os.path import join as ospathjoin

from typing import List

from janis_assistant.data.enums import TaskStatus
from janis_assistant.data.models.workflow import WorkflowModel
from janis_assistant.data.models.workflowjob import WorkflowJobModel
from janis_assistant.data.providers.jobdbprovider import JobDbProvider
from janis_assistant.data.providers.outputdbprovider import OutputDbProvider
from janis_assistant.data.providers.progressdbprovider import ProgressDbProvider
from janis_assistant.data.providers.rundbprovider import RunDbProvider
from janis_assistant.data.providers.versionsdbprovider import VersionsDbProvider
from janis_assistant.data.providers.workflowmetadataprovider import (
    WorkflowMetadataDbProvider,
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

    def __init__(self, wid: str, path: str, readonly=False):
        self.exec_path = path
        self.readonly = readonly

        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        sqlpath = self.get_sql_path()
        self.runs = RunDbProvider(db=self.connection, cursor=self.cursor)
        self.workflowmetadata = WorkflowMetadataDbProvider(
            sqlpath, wid=wid, readonly=readonly
        )
        self.progressDB = ProgressDbProvider(
            db=self.connection, cursor=self.cursor, wid=wid
        )

        self.outputsDB = OutputDbProvider(
            db=self.connection, cursor=self.cursor, wid=wid
        )
        self.jobsDB = JobDbProvider(db=self.connection, cursor=self.cursor, wid=wid)
        self.versionsDB = VersionsDbProvider(dblocation=sqlpath, readonly=readonly)

    @staticmethod
    def get_workflow_metadatadb(execpath, wid, readonly=False):

        connection = None
        sqlpath = WorkflowDbManager.get_sql_path_base(execpath)

        if not wid:

            Logger.debug("Opening database connection to get wid from: " + sqlpath)
            try:
                connection = sqlite3.connect(f"file:{sqlpath}?mode=ro", uri=True)
            except:
                Logger.critical("Error when opening DB connection to: " + sqlpath)
                raise

            wid = RunDbProvider(db=connection, cursor=connection.cursor()).get_latest()
            if not wid:
                raise Exception("Couldn't get WID in task directory")

        retval = WorkflowMetadataDbProvider(sqlpath, wid, readonly=readonly)
        if connection:
            connection.close()
        return retval

    @staticmethod
    def get_latest_workflow(path) -> str:
        try:
            connection = sqlite3.connect(
                f"file:{WorkflowDbManager.get_sql_path_base(path)}?mode=ro", uri=True
            )
            runDb = RunDbProvider(db=connection, cursor=connection.cursor())
            return runDb.get_latest()

        except:
            Logger.critical("Error when opening DB connection to: " + path)
            raise

    @staticmethod
    def get_sql_path_base(exec_path):
        return ospathjoin(exec_path, "janis/task.db")

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

    def save_metadata(self, metadata: WorkflowModel):

        # mfranklin: DO NOT UPDATE THE STATUS HERE!

        # Let's just say the actual workflow metadata has to updated separately
        alljobs = self.flatten_jobs(metadata.jobs or [])
        self.jobsDB.update_or_insert_many(alljobs)

        self.workflowmetadata.last_updated = DateUtil.now()
        if metadata.error:
            self.workflowmetadata.error = metadata.error
        if metadata.execution_dir:
            self.workflowmetadata.execution_dir = metadata.execution_dir

        if metadata.finish:
            self.workflowmetadata.finish = metadata.finish
        return

    def get_metadata(self):
        jobs = self.jobsDB.get_all_mapped()
        return WorkflowModel(
            wid=self.workflowmetadata.wid,
            engine_wid=self.workflowmetadata.engine_wid,
            name=self.workflowmetadata.name,
            start=self.workflowmetadata.start,
            finish=self.workflowmetadata.finish,
            outdir=self.exec_path,
            execution_dir=self.workflowmetadata.execution_dir,
            status=self.workflowmetadata.status,
            engine=self.workflowmetadata.engine.description(),
            engine_url=self.workflowmetadata.engine_url,
            # filesystem=self.workflowmetadata.filesystem.id(),
            labels=self.workflowmetadata.labels,
            error=self.workflowmetadata.error,
            author=self.workflowmetadata.author,
            jobs=jobs,
            last_updated=self.workflowmetadata.last_updated,
            outputs=(
                self.outputsDB.get_all()
                if self.workflowmetadata.status == TaskStatus.COMPLETED
                else None
            ),
        )

    def flatten_jobs(self, jobs: List[WorkflowJobModel]):
        flattened = jobs
        for j in jobs:
            if j.jobs:
                flattened.extend(self.flatten_jobs(j.jobs))
        return flattened

    def commit(self):
        if self.connection:
            self.connection.commit()
        else:
            Logger.critical("Couldn't commit to DB connection")

    def close(self):
        self.connection.close()
        self.cursor = None
        self.connection = None
