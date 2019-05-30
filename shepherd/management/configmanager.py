import os
import sqlite3
from enum import Enum
from typing import Dict, Optional

import janis

from shepherd.data.filescheme import FileScheme
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
        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.create_tasks_table_if_required()
        self.create_environment_related_tables_if_required()

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

    def create_tasks_table_if_required(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS tasks(
            tid varchar(6) PRIMARY KEY, 
            outputdir text
        )""")
        self.commit()

    def create_environment_related_tables_if_required(self):
        self.create_engine_table_if_required()
        self.create_filescheme_table_if_required()
        self.create_environment_table_if_required()

    def create_environment_table_if_required(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS environments(
            envid varchar(10) PRIMARY KEY, 
            engid varchar(10), 
            fsid varchar(10),
            FOREIGN KEY(engid)  REFERENCES engines(engid),
            FOREIGN KEY(fsid)   REFERENCES fileschemes(fsid)
        )""")

    def create_engine_table_if_required(self):
        # essentially a key-value store
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS engines(
            engid varchar(10), 
            key varchar(15), 
            value text, 
            PRIMARY KEY(engid, key)
        )""")

    def create_filescheme_table_if_required(self):
        # essentially a key-value store
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS fileschemes(
            fsid varchar(10),
            key varchar(15),
            value text, 
            PRIMARY KEY(fsid, key)
        )""")

    def create_task(self, wf: janis.Workflow, environment: Environment, hints: Dict[str, str],
                   validation_requirements: Optional[ValidationRequirements], outdir=None):

        od = outdir if outdir else EnvVariables.exec_dir.default()

        forbiddenids = set(t[0] for t in self.cursor.execute("SELECT tid FROM tasks").fetchall())
        forbiddenids = forbiddenids.union(set(os.listdir(od)))

        tid = generate_new_id(forbiddenids)
        Logger.info(f"Starting task with id = '{tid}'")

        task_path = od + ("" if outdir else (tid + "/"))
        self._add_task(tid, task_path)

        return TaskManager.from_janis(tid, outdir=task_path, wf=wf, environment=environment, hints=hints,
                                      validation_requirements=validation_requirements)

    def from_tid(self, tid):
        path = self.cursor.execute("SELECT outputdir FROM tasks where tid=?", (tid, )).fetchone()
        if not path:
            raise Exception(f"Couldn't find task with id='{tid}'")
        return TaskManager.from_path(path[0])

    def _add_task(self, tid, outdir):
        self.cursor.execute("INSERT INTO tasks VALUES (?, ?)", (tid, outdir))
        self.commit()

    def persist_environment(self, environment, should_commit=True):
        # save engine
        self.persist_engine(environment.id(), environment.engine, should_commit=False)
        # save filescheme
        self.persist_filescheme(environment.id(), environment.filescheme, should_commit=False)

        # save environment
        self.cursor.execute("""INSERT INTO environments (envid, engid, fsid) VALUES (?, ?, ?)""",
                            (environment.id(), environment.engine.id, environment.filescheme.id))

        if should_commit:
            self.commit()

    def persist_engine(self, envid, engine, should_commit=True):
        # check if already exists
        kvargs = engine.db_to_kwargs()
        for (k, v) in kvargs:
            self.cursor.execute("""INSERT INTO engines (envid, key, value) VALUES (?, ?, ?)""",
                                (envid, k, v))

        if should_commit:
            self.commit()

    def persist_filescheme(self, envid, filescheme, should_commit=True):
        # check if already exists

        kvargs = filescheme.db_to_kwargs()
        for (k,v) in kvargs.items():
            self.cursor.execute("""INSERT INTO fileschemes (envid, key, value) VALUES (?, ?, ?)""",
                                (filescheme.id, k, str(v)))

        if should_commit:
            self.commit()

    def get_filescheme(self, fsid) -> FileScheme:
        args = self.cursor.execute("SELECT key, value FROM fileschemes where fsid = ?", (fsid, )).fetchall()
        arged = {k: v for k, v in args}
        FS = FileScheme.get_type(arged["fstype"])
        return FS.db_from_kwargs(**arged)

    def get_engine(self, engid) -> FileScheme:
        args = self.cursor.execute("SELECT key, value FROM engines where engid = ? ", (engid, )).fetchall()
        arged = {k: v for k, v in args}
        FS = get_engine_type(engid)
        return FS.db_from_kwargs(**arged)

    # def _save_default_configs(self):



