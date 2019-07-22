import os
import sqlite3
from enum import Enum
from typing import Dict, Optional, cast, List, Tuple

import janis

from janis_runner.data.models.filescheme import FileScheme
from janis_runner.data.providers.config.enginedbprovider import EngineDbProvider
from janis_runner.data.providers.config.environmentdbprovider import EnvironmentDbProvider
from janis_runner.data.providers.config.fileschemedbprovider import FileschemeDbProvider
from janis_runner.data.providers.config.tasksdbprovider import TasksDbProvider, TaskRow
from janis_runner.engines import Engine
from janis_runner.environments.environment import Environment
from janis_runner.management.taskmanager import TaskManager
from janis_runner.utils import Logger, generate_new_id
from janis_runner.validation import ValidationRequirements


class EnvVariables(Enum):
    config_dir = "JANIS_CONFIGDIR"
    exec_dir = "JANIS_OUTPUTDIR"
    search_path = "JANIS_SEARCHPATH"

    def __str__(self):
        return self.value

    def default(self):
        import os

        if self == EnvVariables.config_dir:
            return os.getenv("HOME") + "/.janis/"
        elif self == EnvVariables.exec_dir:
            return os.getenv("HOME") + "/janis-runner/"

        raise Exception(f"Couldn't determine default() for '{self.value}'")


class ConfigManager:

    _manager = None

    @staticmethod
    def manager():
        if not ConfigManager._manager:
            ConfigManager._manager = ConfigManager()
        return ConfigManager._manager

    def __init__(self):
        self._path = self.get_db_path()

        self.is_new = not os.path.exists(self._path)

        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.taskDB = TasksDbProvider(self.connection, self.cursor)
        self.environmentDB = EnvironmentDbProvider(self.connection, self.cursor)
        self.engineDB = EngineDbProvider(self.connection, self.cursor)
        self.fileschemeDB = FileschemeDbProvider(self.connection, self.cursor)

        if self.is_new:
            self.insert_default_environments()

    @staticmethod
    def get_db_path():
        Logger.log(
            f"Looking for db_path in environment variable: '${EnvVariables.config_dir}'"
        )
        dbpath = os.getenv(EnvVariables.config_dir.value)
        if not dbpath:
            Logger.log("Couldn't find environment variable, defaulting $HOME")
            dbpath = EnvVariables.config_dir.default()

        if not os.path.exists(dbpath):
            os.makedirs(dbpath)

        extra_slash = "" if dbpath.endswith("/") else "/"

        return dbpath + extra_slash + "janis.db"

    def db_connection(self):
        Logger.log("Opening database connection to: " + self._path)
        return sqlite3.connect(self._path)

    def commit(self):
        return self.connection.commit()

    def create_task(
        self,
        wf: janis.Workflow,
        environment: Environment,
        hints: Dict[str, str],
        validation_requirements: Optional[ValidationRequirements],
        outdir=None,
        inputs_dict: dict = None,
        dryrun=False,
        watch=True,
    ) -> TaskManager:

        od = outdir if outdir else EnvVariables.exec_dir.default()

        forbiddenids = set(
            t[0] for t in self.cursor.execute("SELECT tid FROM tasks").fetchall()
        )
        forbiddenids = forbiddenids.union(set(os.listdir(od)))

        tid = generate_new_id(forbiddenids)
        Logger.info(f"Starting task with id = '{tid}'")

        task_path = od + ("" if outdir else (tid + "/"))
        self.taskDB.insert_task(TaskRow(tid, task_path))

        return TaskManager.from_janis(
            tid,
            outdir=task_path,
            wf=wf,
            environment=environment,
            hints=hints,
            inputs_dict=inputs_dict,
            validation_requirements=validation_requirements,
            dryrun=dryrun,
            watch=watch,
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

    def query_tasks(self, status, env):
        tids: [TaskRow] = self.taskDB.get_all_tasks()
        ms: List[Tuple[TaskRow, TaskManager]] = [
            (t, TaskManager.from_path(t.outputdir, self)) for t in tids
        ]
        return [t.tid for (t, m) in ms if m.has(status=status, environment=env)]
