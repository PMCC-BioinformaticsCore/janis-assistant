import os
import sqlite3
from datetime import datetime
from shutil import rmtree
from typing import Dict, Optional, Union, List
from contextlib import contextmanager

from janis_assistant.management.envvariables import EnvVariables

from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.engines import Engine
from janis_core import Workflow, Logger, Tool

from janis_assistant.data.models.run import RunModel
from janis_assistant.data.providers.janisdbprovider import TasksDbProvider, TaskRow
from janis_assistant.management.configuration import JanisConfiguration
from janis_assistant.management.workflowmanager import WorkflowManager
from janis_assistant.utils import generate_new_id, fully_qualify_filename
from janis_assistant.utils.batchrun import BatchRunRequirements
from janis_assistant.validation import ValidationRequirements


class ConfigManager:

    # _manager = None

    # @staticmethod
    # def manager():
    #     if not ConfigManager._manager:
    #         ConfigManager._manager = ConfigManager()
    #     return ConfigManager._manager

    def __init__(self, db_path: Optional[str], readonly=False):

        self.readonly = readonly
        if not db_path:
            config_dir = EnvVariables.config_dir.resolve(True)
            Logger.log(
                f"db_path wasn't provided to config manager, using config_dir: '{config_dir}/janis.db'"
            )
            db_path = fully_qualify_filename(os.path.join(config_dir, "janis.db"))
        self.db_path = db_path
        self.is_new = not os.path.exists(db_path)

        cp = os.path.dirname(db_path)
        os.makedirs(cp, exist_ok=True)

        self._connection: Optional[sqlite3.Connection] = None
        self._taskDB: Optional[TasksDbProvider] = None

    @staticmethod
    def get_from_path_or_submission_lazy(
        submission_id, readonly: bool, db_path: Optional[str] = None,
    ):
        """
        2020-10-01 mfranklin:
            Probably the method you want to get a WorkflowManager from submissionID:

        :return: WorkflowManager of the submission_id (or THROWS)
        """
        expanded_path = fully_qualify_filename(submission_id)
        if os.path.exists(expanded_path):
            return WorkflowManager.from_path_get_latest_manager(
                expanded_path, readonly=readonly
            )

        return ConfigManager(
            db_path=db_path, readonly=True
        ).get_from_path_or_submission(
            submission_id=submission_id, readonly=readonly, perform_path_check=False
        )

    def get_from_path_or_submission(
        self, submission_id, readonly: bool, perform_path_check=True
    ):
        if perform_path_check:
            expanded_path = fully_qualify_filename(submission_id)
            if os.path.exists(expanded_path):
                return WorkflowManager.from_path_get_latest_manager(
                    expanded_path, readonly=readonly
                )

        potential_submission = self.get_lazy_db_connection().get_by_id(submission_id)
        if potential_submission:
            return WorkflowManager.from_path_with_submission_id(
                potential_submission.execution_dir,
                submission_id=submission_id,
                readonly=readonly,
            )

        raise Exception(
            f"Couldn't find task with id='{submission_id}', and no directory was found "
        )

    def get_lazy_db_connection(self):
        if self._taskDB is None:
            self._connection = self.db_connection()

            self._taskDB = TasksDbProvider(self._connection, readonly=self.readonly)

        return self._taskDB

    @contextmanager
    def with_cursor(self):
        cursor = self.get_lazy_db_connection()._db.cursor()
        yield cursor
        cursor.close()

    def db_connection(self):
        try:
            if self.readonly:
                Logger.debug(
                    "Opening database connection to in READONLY mode: " + self.db_path
                )
                return sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)

            Logger.debug("Opening database connection: " + self.db_path)
            return sqlite3.connect(self.db_path)
        except:
            Logger.critical("Error when opening DB connection to: " + self.db_path)
            raise

    def commit(self):
        self.get_lazy_db_connection()
        return self._connection.commit()

    def remove_task(self, task: Union[str, TaskRow], keep_output: bool):
        if isinstance(task, str):
            wid = task
            task = self.get_lazy_db_connection().get_by_wid(task)
            if task is None:
                raise Exception("Couldn't find workflow with ID = " + wid)

        tm = WorkflowManager.from_path_with_submission_id(
            task.outputdir, task.submission_id
        )
        tm.cleanup_execution()
        tm.database.close()

        if not keep_output and os.path.exists(task.outputdir):
            Logger.info("Removing " + task.outputdir)
            rmtree(task.outputdir)
        else:
            Logger.info("Skipping output dir deletion, can't find: " + task.outputdir)

        self.get_lazy_db_connection().remove_by_id(task.submission_id)
        Logger.info("Deleted task: " + task.submission_id)

    def get_ids(self, db_path):
        try:
            with self.with_cursor() as cursor:
                return set(
                    t[0] for t in cursor.execute("SELECT id FROM tasks").fetchall()
                )
        except sqlite3.OperationalError as e:
            if "no such column: id" in repr(e):
                from shutil import move

                dt = datetime.utcnow()
                np = f"{db_path}.original-{dt.strftime('%Y%m%d')}"
                Logger.warn(f"Moving old janis-db to '{np}'")
                move(db_path, np)
                self._taskDB = None
                return self.get_ids(db_path)
            raise

    def create_task_base(self, wf: Workflow, job: PreparedJob):

        forbiddenids = set()
        if job.store_in_central_db:
            try:
                with self.with_cursor() as cursor:
                    forbiddenids = set(
                        t[0] for t in cursor.execute("SELECT id FROM tasks").fetchall()
                    )
            except sqlite3.OperationalError as e:
                if "no such column: id" in repr(e):
                    from shutil import move

                    dt = datetime.utcnow()
                    np = f"{job.db_path}.original-{dt.strftime('%Y%m%d')}"
                    Logger.warn(f"Moving old janis-db to '{np}'")
                    move(job.db_path, np)
                    self._taskDB = None
                    return self.create_task_base(wf=wf, job=job)
                raise

        submission_id = generate_new_id(forbiddenids)

        output_dir = fully_qualify_filename(job.output_dir)

        if not job.execution_dir:
            job.execution_dir = os.path.join(output_dir, "janis")
            Logger.debug(
                f"No execution-dir was provided, constructed one from the output-dir: {job.execution_dir}"
            )
        job.execution_dir = fully_qualify_filename(job.execution_dir)

        Logger.info(
            f"Starting task with id = '{submission_id}' | output dir: {job.output_dir} | execution dir: {job.execution_dir}"
        )

        row = TaskRow(
            submission_id, execution_dir=job.execution_dir, output_dir=output_dir
        )
        WorkflowManager.create_dir_structure(job.execution_dir)

        if job.store_in_central_db:
            self.get_lazy_db_connection().insert_task(row)
        else:
            Logger.info(
                f"Not storing task '{submission_id}' in database. To watch, use: 'janis watch {output_dir}'"
            )

        if self._connection:
            self._connection.commit()
            self._connection.close()
            self._taskDB = None
            self._connection = None
        return row

    def get_row_for_submission_id_or_path(self, submission_id) -> TaskRow:

        potential_submission = self.get_lazy_db_connection().get_by_id(submission_id)
        if potential_submission:
            return potential_submission

        expanded_path = fully_qualify_filename(submission_id)
        if os.path.exists(expanded_path):
            (execpath, sid) = WorkflowManager.from_path_get_latest_submission_id(
                expanded_path
            )
            return TaskRow(
                execution_dir=execpath,
                submission_id=sid,
                output_dir=None,
                timestamp=None,
            )

        raise Exception(
            f"Couldn't find task with id='{submission_id}', and no directory was found."
        )

    # def from_submission_id_or_path(self, submission_id, readonly: bool):
    #
    #     expanded_path = fully_qualify_filename(submission_id)
    #     if os.path.exists(expanded_path):
    #         return WorkflowManager.from_path_get_latest_manager(
    #             expanded_path, readonly=readonly
    #         )
    #
    #     # Get from central database (may run into lock errors though)
    #     potential_submission = self.get_lazy_db_connection().get_by_id(submission_id)
    #     if potential_submission:
    #
    #         return WorkflowManager.from_path_with_submission_id(
    #             potential_submission.execution_dir,
    #             submission_id=submission_id,
    #             readonly=readonly,
    #         )
    #
    #     raise Exception(
    #         f"Couldn't find task with id='{submission_id}', and no directory was found "
    #     )

    def query_tasks(self, status, name) -> Dict[str, RunModel]:

        rows: List[TaskRow] = self.get_lazy_db_connection().get_all_tasks()

        failed = []
        relevant = {}

        for row in rows:
            if not os.path.exists(row.execution_dir):
                failed.append(row.submission_id)
                continue
            try:
                metadb = WorkflowManager.has(
                    row.execution_dir,
                    submission_id=row.submission_id,
                    name=name,
                    status=status,
                )
                if metadb:
                    model = metadb
                    model.outdir = row.output_dir
                    relevant[row.submission_id] = model
            except Exception as e:
                Logger.critical(f"Couldn't check workflow '{row.submission_id}': {e}")
                failed.append(row.submission_id)

        if failed:
            failedstr = ", ".join(failed)
            Logger.warn(
                f"Couldn't get information for tasks: {failedstr}, run"
                f"'janis cleanup' to clean up your tasks."
            )

        return relevant

    def cleanup_missing_tasks(self):
        from tabulate import tabulate

        rows: [TaskRow] = self.get_lazy_db_connection().get_all_tasks()

        failed = []

        for row in rows:
            if not os.path.exists(row.outputdir):
                failed.append((row.submission_id, row.outputdir))
                continue
            try:
                _ = WorkflowManager.from_path_with_submission_id(
                    row.outputdir, row.submission_id, readonly=True
                )
            except Exception as e:
                failed.append((row.submission_id, row.outputdir))

        if failed:
            Logger.warn(f"Removing the following tasks:\n" + tabulate(failed))

            if "y" in str(input(f"Remove {len(failed)} tasks (Y / n)? ")).lower():
                self.get_lazy_db_connection().remove_by_ids([r[0] for r in failed])
                Logger.info("Cleaned up tasks")
            else:
                Logger.info("Skipping cleaning of tasks")
