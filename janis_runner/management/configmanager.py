import os
import sqlite3
from datetime import datetime
from shutil import rmtree
from typing import Dict, Optional, Union

from janis_core import Workflow, Logger

from janis_runner.data.models.workflow import WorkflowModel
from janis_runner.data.providers.janisdbprovider import TasksDbProvider, TaskRow
from janis_runner.environments.environment import Environment
from janis_runner.management.configuration import JanisConfiguration
from janis_runner.management.workflowmanager import WorkflowManager
from janis_runner.utils import generate_new_id
from janis_runner.validation import ValidationRequirements


class ConfigManager:

    _manager = None

    @staticmethod
    def manager():
        if not ConfigManager._manager:
            ConfigManager._manager = ConfigManager()
        return ConfigManager._manager

    def __init__(self):

        # Before the manager() is called, someone (the CLI definitely) MUST call
        # JanisConfiguration.inital_configuration(potential_config_paths), this
        # will search os.env for potential configs
        config = JanisConfiguration.manager()
        self.is_new = not os.path.exists(config.dbpath)

        cp = os.path.dirname(config.dbpath)
        os.makedirs(cp, exist_ok=True)
        os.makedirs(config.outputdir, exist_ok=True)

        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.taskDB = TasksDbProvider(self.connection, self.cursor)
        # self.environmentDB = EnvironmentDbProvider(self.connection, self.cursor)
        # self.engineDB = EngineDbProvider(self.connection, self.cursor)
        # self.fileschemeDB = FileschemeDbProvider(self.connection, self.cursor)

        # if self.is_new:
        #     self.insert_default_environments()

    def db_connection(self):
        config = JanisConfiguration.manager()
        Logger.log("Opening database connection to: " + config.dbpath)
        return sqlite3.connect(config.dbpath)

    def commit(self):
        return self.connection.commit()

    def remove_task(self, task: Union[str, TaskRow], keep_output: bool):
        if isinstance(task, str):
            wid = task
            task = self.taskDB.get_by_wid(task)
            if task is None:
                raise Exception("Couldn't find workflow with ID = " + wid)

        tm = WorkflowManager.from_path(task.wid)
        tm.remove_exec_dir()
        tm.database.close()

        if not keep_output and os.path.exists(task.outputdir):
            Logger.info("Removing " + task.outputdir)
            rmtree(task.outputdir)
        else:
            Logger.info("Skipping output dir deletion, can't find: " + task.outputdir)

        self.taskDB.remove_by_id(task.wid)
        Logger.info("Deleted task: " + task.wid)

    def create_task_base(self, wf: Workflow, outdir=None):
        config = JanisConfiguration.manager()

        if (
            outdir
            and os.path.exists(outdir)
            and len([l for l in os.listdir(outdir) if not l.startswith(".")]) > 0
        ):
            raise Exception(f"The specified output director '{outdir}' was not empty")

        od = outdir if outdir else os.path.join(config.outputdir, wf.id())

        forbiddenids = set(
            t[0] for t in self.cursor.execute("SELECT wid FROM tasks").fetchall()
        )

        if os.path.exists(od):
            forbiddenids = forbiddenids.union(set(os.listdir(od)))

        wid = generate_new_id(forbiddenids)
        Logger.info(f"Starting task with id = '{wid}'")

        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        task_path = os.path.join(od, "" if outdir else f"{dt}_{wid}/")
        row = TaskRow(wid, task_path)
        WorkflowManager.create_dir_structure(task_path)
        self.taskDB.insert_task(row)

        return row

    def start_task(
        self,
        wid: str,
        wf: Workflow,
        task_path: str,
        environment: Environment,
        hints: Dict[str, str],
        validation_requirements: Optional[ValidationRequirements],
        inputs_dict: dict = None,
        dryrun=False,
        watch=True,
        max_cores=None,
        max_memory=None,
        keep_intermediate_files=False,
        should_disconnect=True,
        skip_mysql=False,
    ) -> WorkflowManager:

        return WorkflowManager.from_janis(
            wid,
            wf=wf,
            outdir=task_path,
            environment=environment,
            hints=hints,
            inputs_dict=inputs_dict,
            validation_requirements=validation_requirements,
            dryrun=dryrun,
            watch=watch,
            max_cores=max_cores,
            max_memory=max_memory,
            keep_intermediate_files=keep_intermediate_files,
            should_disconnect=should_disconnect,
            skip_mysql=skip_mysql,
        )

    def from_wid(self, wid):
        path = self.cursor.execute(
            "SELECT outputdir FROM tasks where wid=?", (wid,)
        ).fetchone()
        if not path:
            raise Exception(f"Couldn't find task with id='{wid}'")
        return WorkflowManager.from_path(path[0])

    def query_tasks(self, status, name) -> Dict[str, WorkflowModel]:

        rows: [TaskRow] = self.taskDB.get_all_tasks()

        failed = []
        relevant = {}

        for row in rows:
            if not os.path.exists(row.outputdir):
                failed.append(row.wid)
                continue
            try:
                metadb = WorkflowManager.has(row.outputdir, name=name, status=status)
                if metadb:
                    model = metadb.to_model()
                    model.outdir = row.outputdir
                    relevant[row.wid] = model
            except:
                failed.append(row.wid)

        if failed:
            failedstr = ", ".join(failed)
            Logger.warn(
                f"Couldn't get information for tasks: {failedstr}, run"
                f"'janis cleanup' to clean up your tasks."
            )

        return relevant
