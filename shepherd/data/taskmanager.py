"""
I think this is where the bread and butter is!

A task manager will have reference to a database,


"""
import os
from typing import Optional, List

import janis

from shepherd.data.dbmanager import DatabaseManager
from shepherd.data.schema import TaskStatus, TaskMetadata
from shepherd.engines import get_ideal_specification_for_engine, AsyncTask
from shepherd.environments.environment import Environment
from shepherd.management import get_default_export_dir, generate_new_id
from shepherd.validation import generate_validation_workflow_from_janis, ValidationRequirements


class TaskManager:

    def __init__(self, tid, environment: Environment = None):
        # do stuff here
        self.tid = tid
        self.database = DatabaseManager(tid, path=self.get_task_path())

        # hydrate from here if required
        self.create_output_structure()
        self._engine_tid = None

        if environment:
            self.environment = environment
        else:
            # get environment from db
            env = self.database.get_meta_info(DatabaseManager.InfoKeys.environment)
            if not env:
                raise Exception(f"Couldn't get environment from DB for task id: '{self.tid}'")

            # will except if not valid, but should probably pull store + pull more metadata from env
            #
            # If we do store more metadata, it might be worth storing a hash of the
            # environment object to ensure we're getting the same environment back.
            Environment.get_predefined_environment_by_id(env)

    @staticmethod
    def from_janis(wf: janis.Workflow, environment: Environment,
                   validation_requirements: Optional[ValidationRequirements]):

        # create tid
        # create output folder
        # create structure

        tid = generate_new_id()
        # output directory has been created

        tm = TaskManager(tid, environment=environment)
        tm.database.add_meta_infos([
            (DatabaseManager.InfoKeys.status, TaskStatus.PROCESSING),
            (DatabaseManager.InfoKeys.validating, validation_requirements is not None),
            (DatabaseManager.InfoKeys.engineId, environment.engine.id()),
            (DatabaseManager.InfoKeys.environment, environment.id())
        ])

        # persist environment && validation_requirements

        # write jobs
        output_dir = tm.get_task_path()
        spec = get_ideal_specification_for_engine(environment.engine)

        wf.translate(spec, export_path=output_dir)

        workflow_to_evaluate = wf
        if validation_requirements:
            # we need to generate both the validation and non-validation workflow
            workflow_to_evaluate = generate_validation_workflow_from_janis(wf, validation_requirements)
            workflow_to_evaluate.translate(spec, export_path=output_dir)

        AsyncTask(environment.engine)

    def get_engine_tid(self):
        if not self._engine_tid:
            self._engine_tid = self.database.get_meta_info(DatabaseManager.InfoKeys.engine_tid)
        return self._engine_tid


    # @staticmethod
    def get_task_path(self):
        return get_default_export_dir() + "/" + self.tid + "/"

    def get_task_path_safe(self):
        path = self.get_task_path()
        TaskManager._create_dir_if_needed(path)
        return path

    def create_output_structure(self):
        outputdir = self.get_task_path_safe()
        folders = [
            "workflow",
            "metadata"
        ]

        if self.is_validating:
            folders.append("validation")

        # workflow folder
        for f in folders:
            self._create_dir_if_needed(outputdir + f)

    def watch(self):
        import time
        from subprocess import call
        status = None

        while status not in TaskStatus.FINAL_STATES():
            meta = self.metadata()
            call('clear')
            print(meta.format())
            status = meta.status
            if status not in TaskStatus.FINAL_STATES():
                time.sleep(2)

    def metadata(self) -> TaskMetadata:
        return self.environment.engine.metadata(self.get_engine_tid())

    @staticmethod
    def _create_dir_if_needed(path):
        if not os.path.exists(path):
            os.makedirs(path)
