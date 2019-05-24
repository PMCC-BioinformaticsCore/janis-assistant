import os
import sqlite3
from enum import Enum
from typing import Dict, Optional

import janis

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
            return os.getenv("HOME") + "/janis-runner/"
        elif self == EnvVariables.exec_dir:
            return os.getenv("HOME") + "/janis-runner/"
        elif self == EnvVariables.exec_dir:
            return None

        raise Exception(f"Couldn't determine default() for '{self.value}'")


class ConfigManager:

    def __init__(self):
        self._path = self.get_db_path()
        self.connection = self.db_connection()
        self.cursor = self.connection.cursor()

        self.create_tasks_table_if_required()

    @staticmethod
    def get_db_path():
        Logger.log(f"Looking for db_path in environment variable: '${EnvVariables.config_dir}'")
        dbpath = os.getenv(EnvVariables.config_dir.value)
        if not dbpath:
            Logger.log("Couldn't find environment variable, defaulting $HOME")
            dbpath = EnvVariables.config_dir.default()

        return dbpath + "/janis.db"

    def db_connection(self):
        Logger.log("Opening database connection to: " + self._path)
        return sqlite3.connect(self._path)

    def commit(self):
        return self.connection.commit()

    def create_tasks_table_if_required(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS tasks
                            (tid text PRIMARY KEY, outputdir text)""")
        self.commit()

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

