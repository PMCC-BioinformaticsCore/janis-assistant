import os
import sqlite3
from datetime import datetime
from shutil import rmtree
from typing import Dict, Optional, Union, List
from contextlib import contextmanager

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

    _manager = None

    @staticmethod
    def manager():
        if not ConfigManager._manager:
            ConfigManager._manager = ConfigManager()
        return ConfigManager._manager

    def __init__(self, readonly=False):

        # Before the manager() is called, someone (the CLI definitely) MUST call
        # JanisConfiguration.inital_configuration(potential_config_paths), this
        # will search os.env for potential configs
        config = JanisConfiguration.manager()
        self.readonly = readonly
        self.is_new = not os.path.exists(config.dbpath)

        cp = os.path.dirname(config.dbpath)
        os.makedirs(cp, exist_ok=True)
        if config.outputdir:
            os.makedirs(config.outputdir, exist_ok=True)

        self._connection: Optional[sqlite3.Connection] = None
        self._taskDB: Optional[TasksDbProvider] = None

    def get_lazy_db_connection(self):
        if self._taskDB is None:
            self._connection = self.db_connection()

            self._taskDB = TasksDbProvider(self._connection, readonly=self.readonly)

        return self._taskDB

    @contextmanager
    def with_cursor(self):
        yield self.get_lazy_db_connection()._db.cursor()

    def db_connection(self):
        config = JanisConfiguration.manager()
        try:
            if self.readonly:
                Logger.debug(
                    "Opening database connection to in READONLY mode: " + config.dbpath
                )
                return sqlite3.connect(f"file:{config.dbpath}?mode=ro", uri=True)

            Logger.debug("Opening database connection: " + config.dbpath)
            return sqlite3.connect(config.dbpath)
        except:
            Logger.critical("Error when opening DB connection to: " + config.dbpath)
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

    def create_task_base(
        self, wf: Workflow, outdir=None, execution_dir=None, store_in_centraldb=True
    ):
        config = JanisConfiguration.manager()

        """
        If you don't spec
        
        """

        if not outdir and not config.outputdir:
            raise Exception(
                f"You must specify an output directory (or specify an '{JanisConfiguration.Keys.OutputDir.value}' "
                f"in your configuration)"
            )

        default_outdir = None

        if config.outputdir:
            default_outdir = os.path.join(config.outputdir, wf.id())

        forbiddenids = set()
        if store_in_centraldb:
            try:
                with self.with_cursor() as cursor:
                    forbiddenids = set(
                        t[0] for t in cursor.execute("SELECT id FROM tasks").fetchall()
                    )
            except sqlite3.OperationalError as e:
                if "no such column: id" in repr(e):
                    from shutil import move

                    config = JanisConfiguration.manager()
                    dt = datetime.utcnow()
                    np = f"{config.dbpath}.original-{dt.strftime('%Y%m%d')}"
                    Logger.warn(f"Moving old janis-db to '{np}'")
                    move(config.dbpath, np)
                    self._taskDB = None
                    return self.create_task_base(
                        wf,
                        outdir=outdir,
                        execution_dir=execution_dir,
                        store_in_centraldb=store_in_centraldb,
                    )
                raise
        if outdir:
            if os.path.exists(outdir):
                # this should theoretically scoop through all the ones in the taskDB and
                # add them to the forbidden ones, though this might cause more issues for now.
                forbiddenids = forbiddenids.union(set(os.listdir(outdir)))
        else:
            if os.path.exists(default_outdir):
                forbiddenids = forbiddenids.union(set(os.listdir(default_outdir)))

        submission_id = generate_new_id(forbiddenids)

        output_dir = outdir
        if not output_dir:
            od = default_outdir
            dt = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = os.path.join(od, f"{dt}_{submission_id}/")

        output_dir = fully_qualify_filename(output_dir)

        if not execution_dir:
            execution_dir = os.path.join(output_dir, "janis")
            Logger.debug(
                f"No execution-dir was provided, constructed one from the output-dir: {execution_dir}"
            )
        execution_dir = fully_qualify_filename(execution_dir)

        Logger.info(
            f"Starting task with id = '{submission_id}' | output dir: {output_dir} | execution dir: {execution_dir}"
        )

        row = TaskRow(submission_id, execution_dir=execution_dir, output_dir=output_dir)
        WorkflowManager.create_dir_structure(execution_dir)

        if store_in_centraldb:
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

    def start_task(
        self,
        submission_id: str,
        tool: Tool,
        output_dir: str,
        execution_dir: str,
        engine: Engine,
        hints: Dict[str, str],
        validation_requirements: Optional[ValidationRequirements],
        batchrun_requirements: Optional[BatchRunRequirements],
        inputs_dict: dict = None,
        dryrun=False,
        watch=True,
        max_cores=None,
        max_memory=None,
        max_duration=None,
        keep_intermediate_files=False,
        run_in_background=True,
        dbconfig=None,
        allow_empty_container=False,
        container_override: dict = None,
        check_files=True,
        **kwargs,
    ) -> WorkflowManager:

        return WorkflowManager.from_janis(
            submission_id,
            tool=tool,
            output_dir=output_dir,
            execution_dir=execution_dir,
            engine=engine,
            hints=hints,
            inputs_dict=inputs_dict,
            validation_requirements=validation_requirements,
            batchrun_requirements=batchrun_requirements,
            dryrun=dryrun,
            watch=watch,
            max_cores=max_cores,
            max_memory=max_memory,
            max_duration=max_duration,
            keep_intermediate_files=keep_intermediate_files,
            run_in_background=run_in_background,
            dbconfig=dbconfig,
            allow_empty_container=allow_empty_container,
            container_override=container_override,
            check_files=check_files,
            **kwargs,
        )

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
                execution_dir=expanded_path,
                submission_id=sid,
                output_dir=None,
                timestamp=None,
            )

        raise Exception(
            f"Couldn't find task with id='{submission_id}', and no directory was found."
        )

    def from_submission_id_or_path(self, submission_id, readonly=False):

        self.readonly = readonly

        # Get from central database (may run into lock errors though)
        potential_submission = self.get_lazy_db_connection().get_by_id(submission_id)
        if potential_submission:

            return WorkflowManager.from_path_with_submission_id(
                potential_submission.execution_dir,
                submission_id=submission_id,
                readonly=readonly,
            )

        expanded_path = fully_qualify_filename(submission_id)
        if os.path.exists(expanded_path):
            return WorkflowManager.from_path_get_latest_manager(
                expanded_path, readonly=readonly
            )

        raise Exception(
            f"Couldn't find task with id='{submission_id}', and no directory was found "
        )

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
