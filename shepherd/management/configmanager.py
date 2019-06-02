import os
import sqlite3
from enum import Enum
from typing import Dict, Optional, cast

import janis

from shepherd.data.models.filescheme import FileScheme
from shepherd.data.providers.config.enginedbprovider import EngineDbProvider
from shepherd.data.providers.config.environmentdbprovider import EnvironmentDbProvider
from shepherd.data.providers.config.fileschemedbprovider import FileschemeDbProvider
from shepherd.data.providers.config.taskdbprovider import TaskDbProvider, TaskRow
from shepherd.engines import get_engine_type
from shepherd.environments.environment import Environment
from shepherd.management.taskmanager import TaskManager
from shepherd.utils import Logger, generate_new_id
from shepherd.validation import ValidationRequirements


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

    def __init__(self):
        self._path = self.get_db_path()

        self.is_new = not os.path.exists(self._path)

        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.taskDB = TaskDbProvider(self.connection, self.cursor)
        self.environmentDB = EnvironmentDbProvider(self.connection, self.cursor)
        self.engineDB = EngineDbProvider(self.connection, self.cursor)
        self.fileschemeDB = FileschemeDbProvider(self.connection, self.cursor)

        if self.is_new:
            self.insert_default_environments()

    @staticmethod
    def get_db_path():
        Logger.log(f"Looking for db_path in environment variable: '${EnvVariables.config_dir}'")
        dbpath = os.getenv(EnvVariables.config_dir.value)
        if not dbpath:
            Logger.log("Couldn't find environment variable, defaulting $HOME")
            dbpath = EnvVariables.config_dir.default()

        if not os.path.exists(dbpath):
            os.makedirs(dbpath)

        return dbpath + "/janis.db"

    def db_connection(self):
        Logger.log("Opening database connection to: " + self._path)
        return sqlite3.connect(self._path)

    def commit(self):
        return self.connection.commit()

    def create_environment_related_tables_if_required(self):
        self.create_engine_table_if_required()
        self.create_filescheme_table_if_required()


    def create_engine_table_if_required(self):
        # essentially a key-value store
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS engines(
            engid varchar(10) NOT NULL, 
            key varchar(15) NOT NULL, 
            value text, 
            PRIMARY KEY(engid, key)
        )""")

    def create_filescheme_table_if_required(self):
        # essentially a key-value store
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS fileschemes(
            fsid varchar(10) NOT NULL,
            key varchar(15) NOT NULL,
            value text, 
            PRIMARY KEY(fsid, key)
        )""")

    def create_task(self, wf: janis.Workflow, environment: Environment, hints: Dict[str, str],
                   validation_requirements: Optional[ValidationRequirements], outdir=None, dryrun=False) -> TaskManager:

        od = outdir if outdir else EnvVariables.exec_dir.default()

        forbiddenids = set(t[0] for t in self.cursor.execute("SELECT tid FROM tasks").fetchall())
        forbiddenids = forbiddenids.union(set(os.listdir(od)))

        tid = generate_new_id(forbiddenids)
        Logger.info(f"Starting task with id = '{tid}'")

        task_path = od + ("" if outdir else (tid + "/"))
        self.taskDB.insert_task(TaskRow(tid, task_path))

        return TaskManager.from_janis(tid, outdir=task_path, wf=wf, environment=environment, hints=hints,
                                      validation_requirements=validation_requirements, dryrun=dryrun)

    def from_tid(self, tid):
        path = self.cursor.execute("SELECT outputdir FROM tasks where tid=?", (tid, )).fetchone()
        if not path:
            raise Exception(f"Couldn't find task with id='{tid}'")
        return TaskManager.from_path(path[0])

    def insert_default_environments(self):
        for e in Environment.DEFAULTS():
            self.persist_engine(engine=e.engine, throw_if_exists=False, should_commit=False)
            self.persist_filescheme(filescheme=e.filescheme, throw_if_exists=False, should_commit=False)
            self.environmentDB.persist_environment(e, throw_if_exists=False, should_commit=False)

        self.commit()



    def persist_engine(self, engine, throw_if_exists=True, should_commit=True):
        return self.engineDB.persist(engine, throw_if_exists=throw_if_exists, should_commit=should_commit)

    def persist_filescheme(self, filescheme, throw_if_exists=True, should_commit=True):
        return self.fileschemeDB.persist(filescheme, throw_if_exists=throw_if_exists, should_commit=should_commit)

    def get_filescheme(self, fsid) -> FileScheme:
        return cast(FileScheme, self.fileschemeDB.get(fsid))

    def get_engine(self, engid):
        return self.engineDB.get(engid)

    # def _save_default_configs(self):



