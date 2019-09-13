import os
import sqlite3
from datetime import datetime
from shutil import rmtree

from typing import Dict, Optional, cast, List, Tuple, Union

from janis_core import Workflow
from janis_runner.data.models.filescheme import FileScheme
from janis_runner.data.providers.config.enginedbprovider import EngineDbProvider
from janis_runner.data.providers.config.environmentdbprovider import (
    EnvironmentDbProvider,
)
from janis_runner.data.providers.config.fileschemedbprovider import FileschemeDbProvider
from janis_runner.data.providers.config.tasksdbprovider import TasksDbProvider, TaskRow
from janis_runner.engines import Engine
from janis_runner.environments.environment import Environment
from janis_runner.management.configuration import EnvVariables, JanisConfiguration
from janis_runner.management.taskmanager import TaskManager
from janis_runner.utils import Logger, generate_new_id
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
        os.makedirs(config.executiondir, exist_ok=True)

        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.taskDB = TasksDbProvider(self.connection, self.cursor)
        self.environmentDB = EnvironmentDbProvider(self.connection, self.cursor)
        self.engineDB = EngineDbProvider(self.connection, self.cursor)
        self.fileschemeDB = FileschemeDbProvider(self.connection, self.cursor)

        if self.is_new:
            self.insert_default_environments()

    def db_connection(self):
        config = JanisConfiguration.manager()
        Logger.log("Opening database connection to: " + config.dbpath)
        return sqlite3.connect(config.dbpath)

    def commit(self):
        return self.connection.commit()

    def remove_task(self, task: Union[str, TaskRow], keep_output: bool):
        if isinstance(task, str):
            tid = task
            task = self.taskDB.get_by_tid(task)
            if task is None:
                raise Exception("Couldn't find task with ID = " + tid)

        tm = TaskManager.from_path(task.tid, self)
        tm.remove_exec_dir()
        tm.database.close()

        if not keep_output and os.path.exists(task.outputdir):
            Logger.info("Removing " + task.outputdir)
            rmtree(task.outputdir)
        else:
            Logger.info("Skipping output dir deletion, can't find: " + task.outputdir)

        self.taskDB.remove_by_id(task.tid)
        Logger.info("Deleted task: " + task.tid)

    def create_task_base(self, wf: Workflow, outdir=None):
        config = JanisConfiguration.manager()

        if (
            outdir
            and os.path.exists(outdir)
            and len([l for l in os.listdir(outdir) if not l.startswith(".")]) > 0
        ):
            raise Exception(f"The specified output director '{outdir}' was not empty")

        od = outdir if outdir else os.path.join(config.executiondir, wf.id())

        forbiddenids = set(
            t[0] for t in self.cursor.execute("SELECT tid FROM tasks").fetchall()
        )

        if os.path.exists(od):
            forbiddenids = forbiddenids.union(set(os.listdir(od)))

        tid = generate_new_id(forbiddenids)
        Logger.info(f"Starting task with id = '{tid}'")

        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        task_path = os.path.join(od, "" if outdir else f"{dt}_{tid}/")
        row = TaskRow(tid, task_path)
        TaskManager.create_dir_structure(task_path)
        self.taskDB.insert_task(row)

        return row

    def start_task(
        self,
        tid: str,
        wf: Workflow,
        task_path: str,
        environment: Environment,
        hints: Dict[str, str],
        validation_requirements: Optional[ValidationRequirements],
        inputs_dict: dict = None,
        dryrun=False,
        watch=True,
        show_metadata=True,
        max_cores=None,
        max_memory=None,
        keep_intermediate_files=False,
    ) -> TaskManager:

        return TaskManager.from_janis(
            tid,
            wf=wf,
            outdir=task_path,
            environment=environment,
            hints=hints,
            inputs_dict=inputs_dict,
            validation_requirements=validation_requirements,
            dryrun=dryrun,
            watch=watch,
            show_metadata=show_metadata,
            max_cores=max_cores,
            max_memory=max_memory,
            keep_intermediate_files=keep_intermediate_files,
        )

    def from_tid(self, tid):
        path = self.cursor.execute(
            "SELECT outputdir FROM tasks where tid=?", (tid,)
        ).fetchone()
        if not path:
            raise Exception(f"Couldn't find task with id='{tid}'")
        return TaskManager.from_path(path[0], self)

    def insert_default_environments(self):
        for e in Environment.defaults():
            self.persist_engine(
                engine=e.engine, throw_if_exists=False, should_commit=False
            )
            self.persist_filescheme(
                filescheme=e.filescheme, throw_if_exists=False, should_commit=False
            )
            self.environmentDB.persist_environment(
                e, throw_if_exists=False, should_commit=False
            )

        self.commit()

    def persist_environment_if_required(self, env: Environment):
        try:
            envid = self.get_environment(env.id())

        except KeyError:
            # we didn't find the environment
            self.get_engine(env.engine)

    def get_environment(self, envid):
        envtuple = self.environmentDB.get_by_id(envid)
        if not envtuple:
            raise KeyError(f"Couldn't find environment with id '{envid}'")

        (engid, fsid) = envtuple
        eng: Engine = self.engineDB.get(engid)
        fs: FileScheme = self.fileschemeDB.get(fsid)

        return Environment(envid, eng, fs)

    def persist_engine(self, engine, throw_if_exists=True, should_commit=True):
        return self.engineDB.persist(
            engine, throw_if_exists=throw_if_exists, should_commit=should_commit
        )

    def persist_filescheme(self, filescheme, throw_if_exists=True, should_commit=True):
        return self.fileschemeDB.persist(
            filescheme, throw_if_exists=throw_if_exists, should_commit=should_commit
        )

    def get_filescheme(self, fsid) -> FileScheme:
        return cast(FileScheme, self.fileschemeDB.get(fsid))

    def get_engine(self, engid):
        return self.engineDB.get(engid)

    def query_tasks(self, status, name) -> Dict[str, TaskManager]:
        rows: [TaskRow] = self.taskDB.get_all_tasks()
        ms = {}
        failed = []
        for row in rows:
            try:
                ms[row.tid] = TaskManager.from_path(row.outputdir, self)
            except:
                failed.append(row.tid)

        if failed:
            failedstr = ", ".join(failed)
            Logger.info(
                f"Couldn't get information for tasks: {failedstr}, run"
                f"'janis cleanup' to clean up your tasks."
            )

        relevant = []

        if not (status or name):
            return ms

        for (t, m) in ms.items():
            if m.has(status=status, name=name):
                relevant.append(t)
        return {t: ms[t] for t in relevant}
