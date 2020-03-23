import os
import sqlite3
from datetime import datetime
from shutil import rmtree
from typing import Dict, Optional, Union

from janis_core import Workflow, Logger

from janis_assistant.data.models.workflow import WorkflowModel
from janis_assistant.data.providers.janisdbprovider import TasksDbProvider, TaskRow
from janis_assistant.environments.environment import Environment
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
        self._cursor: Optional[sqlite3.Cursor] = None
        self._taskDB: Optional[TasksDbProvider] = None
        # self.environmentDB = EnvironmentDbProvider(self.connection, self.cursor)
        # self.engineDB = EngineDbProvider(self.connection, self.cursor)
        # self.fileschemeDB = FileschemeDbProvider(self.connection, self.cursor)

        # if self.is_new:
        #     self.insert_default_environments()

    def get_lazy_db_connection(self):
        if self._taskDB is None:
            self._connection = self.db_connection()
            self._cursor = self._connection.cursor()

            self._taskDB = TasksDbProvider(self._connection, self._cursor)

        return self._taskDB

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

        tm = WorkflowManager.from_path_with_wid(task.outputdir, task.wid)
        tm.remove_exec_dir()
        tm.database.close()

        if not keep_output and os.path.exists(task.outputdir):
            Logger.info("Removing " + task.outputdir)
            rmtree(task.outputdir)
        else:
            Logger.info("Skipping output dir deletion, can't find: " + task.outputdir)

        self.get_lazy_db_connection().remove_by_id(task.wid)
        Logger.info("Deleted task: " + task.wid)

    def create_task_base(self, wf: Workflow, outdir=None, store_in_centraldb=True):
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
            self.get_lazy_db_connection()
            forbiddenids = set(
                t[0] for t in self._cursor.execute("SELECT wid FROM tasks").fetchall()
            )
        if outdir:
            if os.path.exists(outdir):
                # this should theoretically scoop through all the ones in the taskDB and
                # add them to the forbidden ones, though this might cause more issues for now.
                forbiddenids = forbiddenids.union(set(os.listdir(outdir)))
        else:
            if os.path.exists(default_outdir):
                forbiddenids = forbiddenids.union(set(os.listdir(default_outdir)))

        wid = generate_new_id(forbiddenids)

        task_path = outdir
        if not task_path:
            od = default_outdir
            dt = datetime.now().strftime("%Y%m%d_%H%M%S")
            task_path = os.path.join(od, f"{dt}_{wid}/")

        task_path = fully_qualify_filename(task_path)

        Logger.info(f"Starting task with id = '{wid}'")

        row = TaskRow(wid, task_path)
        WorkflowManager.create_dir_structure(task_path)

        if store_in_centraldb:
            self.get_lazy_db_connection().insert_task(row)
        else:
            Logger.info(
                f"Not storing task '{wid}' in database. To watch, use: 'janis watch {task_path}'"
            )

        if self._connection:
            self._connection.commit()
            self._connection.close()
            self._taskDB = None
            self._cursor = None
            self._connection = None
        return row

    def start_task(
        self,
        wid: str,
        wf: Workflow,
        task_path: str,
        environment: Environment,
        hints: Dict[str, str],
        validation_requirements: Optional[ValidationRequirements],
        batchrun_requirements: Optional[BatchRunRequirements],
        inputs_dict: dict = None,
        dryrun=False,
        watch=True,
        max_cores=None,
        max_memory=None,
        keep_intermediate_files=False,
        run_in_background=True,
        mysql=False,
        allow_empty_container=False,
        check_files=True,
    ) -> WorkflowManager:

        return WorkflowManager.from_janis(
            wid,
            wf=wf,
            outdir=task_path,
            environment=environment,
            hints=hints,
            inputs_dict=inputs_dict,
            validation_requirements=validation_requirements,
            batchrun_requirements=batchrun_requirements,
            dryrun=dryrun,
            watch=watch,
            max_cores=max_cores,
            max_memory=max_memory,
            keep_intermediate_files=keep_intermediate_files,
            run_in_background=run_in_background,
            mysql=mysql,
            allow_empty_container=allow_empty_container,
            check_files=check_files,
        )

    def from_wid(self, wid, readonly=False):
        self.readonly = readonly
        self.get_lazy_db_connection()
        path = self._cursor.execute(
            "SELECT outputdir FROM tasks where wid=?", (wid,)
        ).fetchone()
        if not path:
            expanded_path = fully_qualify_filename(wid)
            if os.path.exists(expanded_path):
                return WorkflowManager.from_path_get_latest(
                    expanded_path, readonly=readonly
                )

            raise Exception(f"Couldn't find task with id='{wid}'")
        return WorkflowManager.from_path_with_wid(path[0], wid=wid, readonly=readonly)

    def query_tasks(self, status, name) -> Dict[str, WorkflowModel]:

        rows: [TaskRow] = self.get_lazy_db_connection().get_all_tasks()

        failed = []
        relevant = {}

        for row in rows:
            if not os.path.exists(row.outputdir):
                failed.append(row.wid)
                continue
            try:
                metadb = WorkflowManager.has(
                    row.outputdir, wid=row.wid, name=name, status=status
                )
                if metadb:
                    model = metadb.to_model()
                    model.outdir = row.outputdir
                    relevant[row.wid] = model
            except Exception as e:
                Logger.critical(f"Couldn't check workflow '{row.wid}': {e}")
                failed.append(row.wid)

        if failed:
            failedstr = ", ".join(failed)
            Logger.warn(
                f"Couldn't get information for tasks: {failedstr}, run"
                f"'janis cleanup' to clean up your tasks."
            )

        return relevant
