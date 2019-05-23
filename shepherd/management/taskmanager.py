"""
I think this is where the bread and butter is!

A task manager will have reference to a database,


"""
import os
import time
from subprocess import call
from typing import Optional, List

import janis

from shepherd.management.enums import InfoKeys, ProgressKeys
from shepherd.utils.logger import Logger
from shepherd.data.dbmanager import DatabaseManager
from shepherd.data.filescheme import FileScheme
from shepherd.data.schema import TaskStatus, TaskMetadata
from shepherd.engines import get_ideal_specification_for_engine, AsyncTask, Cromwell
from shepherd.engines.cromwell import CromwellFile
from shepherd.environments.environment import Environment
from shepherd.management import get_default_export_dir, generate_new_id
from shepherd.utils import get_extension
from shepherd.validation import generate_validation_workflow_from_janis, ValidationRequirements


class TaskManager:

    def __init__(self, tid, environment: Environment = None, outdir=None):
        # do stuff here
        self.tid = tid

        # hydrate from here if required
        self._engine_tid = None
        self.outdir = outdir
        self.outdir_workflow = self.get_task_path() + "workflow/"   # handy to have as reference
        self.create_output_structure()

        self.database = DatabaseManager(tid, path=self.get_task_path_safe())
        self.environment = environment

        if environment:
            self.environment = environment
        else:
            # get environment from db
            env = self.database.get_meta_info(InfoKeys.environment)
            if not env:
                raise Exception(f"Couldn't get environment from DB for task id: '{self.tid}'")

            # will except if not valid, but should probably pull store + pull more metadata from env
            #
            # If we do store more metadata, it might be worth storing a hash of the
            # environment object to ensure we're getting the same environment back.
            Environment.get_predefined_environment_by_id(env)

    @staticmethod
    def from_janis(wf: janis.Workflow, environment: Environment,
                   validation_requirements: Optional[ValidationRequirements], output_dir: str = None):

        # create tid
        # create output folder
        # create structure

        tid = generate_new_id()
        # output directory has been created

        tm = TaskManager(tid, environment=environment)
        tm.database.add_meta_infos([
            (InfoKeys.status, TaskStatus.PROCESSING),
            (InfoKeys.validating, validation_requirements is not None),
            (InfoKeys.engineId, environment.engine.id()),
            (InfoKeys.environment, environment.id())
        ])

        spec = get_ideal_specification_for_engine(environment.engine)
        spec_translator = janis.translations.get_translator(spec)
        tm.prepare_and_output_workflow_to_evaluate_if_required(wf, spec_translator, validation_requirements)

        # this happens for all workflows no matter what type
        tm.submit_workflow_if_required(wf, spec_translator)
        tm.resume_if_possible()

    @staticmethod
    def from_tid(tid):
        # get everything and pass to constructor
        # database path
        path = TaskManager.get_task_path_by(tid)
        db = DatabaseManager(tid, path)
        envid = db.get_meta_info(InfoKeys.environment)
        env = Environment.get_predefined_environment_by_id(envid)
        db.close()
        tm = TaskManager(tid, env)
        tm.resume_if_possible()

    def resume_if_possible(self):
        # check status and see if we can resume
        if not self.database.progress_has_completed(ProgressKeys.submitWorkflow):
            return Logger.critical("Can't resume workflow with id '{self.tid}' as the workflow "
                                   "was not submitted to the engine")
        self.wait_if_required()
        self.copy_outputs_if_required()

        print("Finished!")




    def prepare_and_output_workflow_to_evaluate_if_required(self, workflow, translator, validation: ValidationRequirements):
        if self.database.progress_has_completed(ProgressKeys.saveWorkflow):
            return Logger.info(f"Saved workflow from task '{self.tid}', skipping.")

        Logger.log(f"Saving workflow with id '{workflow.id()}'")

        translator.translate(
            workflow,
            to_console=False,
            to_disk=True,
            with_resource_overrides=False,
            merge_resources=False,
            hints=None,
            write_inputs_file=True,
            export_path=self.outdir_workflow)

        Logger.log(f"Saved workflow with id '{workflow.id()}' to '{self.outdir_workflow}'")

        workflow_to_evaluate = workflow
        if validation:
            Logger.log(f"Validation requirements provided, wrapping workflow '{workflow.id()}' with hap.py")

            # we need to generate both the validation and non-validation workflow
            workflow_to_evaluate = generate_validation_workflow_from_janis(workflow, validation)
            translator.translate(
                workflow,
                to_console=False,
                to_disk=True,
                with_resource_overrides=True,
                merge_resources=True,
                hints=None,
                write_inputs_file=True,
                export_path=self.outdir_workflow
            )

        self.database.progress_mark_completed(ProgressKeys.saveWorkflow)
        return workflow_to_evaluate

    def submit_workflow_if_required(self, wf, translator):
        if self.database.progress_has_completed(ProgressKeys.submitWorkflow):
            return Logger.log(f"Workflow '{self.tid}' has submitted finished, skipping")

        fn_wf = self.outdir_workflow + translator.workflow_filename(wf)
        fn_inp = self.outdir_workflow + translator.inputs_filename(wf)
        fn_deps = self.outdir_workflow + translator.dependencies_filename(wf)

        Logger.log(f"Submitting task '{self.tid}' to '{self.environment.engine.id()}'")
        self._engine_tid = self.environment.engine.start_from_paths(fn_wf, fn_inp, fn_deps)
        self.database.add_meta_info(InfoKeys.engine_tid, self._engine_tid)
        Logger.log(f"Submitted workflow ({self.tid}), got engine id = '{self.get_engine_tid()}")

        self.database.progress_mark_completed(ProgressKeys.submitWorkflow)

    def save_metadata_if_required(self):
        if self.database.progress_has_completed(ProgressKeys.savedMetadata):
            return Logger.log(f"Workflow '{self.tid}' has saved metadata, skipping")

        if isinstance(self.environment.engine, Cromwell):
            import json
            meta = self.environment.engine.raw_metadata(self.get_engine_tid()).meta
            with open(self.get_task_path() + "outputs/metadata/metadata.json", "w+") as fp:
                json.dump(meta, fp)

        else:
            raise Exception(f"Don't know how to save metadata for engine '{self.environment.engine.id()}'")

        self.database.progress_mark_completed(ProgressKeys.savedMetadata)

    def copy_outputs_if_required(self):
        if self.database.progress_has_completed(ProgressKeys.copiedOutputs):
            return Logger.log(f"Workflow '{self.tid}' has copied outputs, skipping")

        outputs = self.environment.engine.outputs_task(self.get_engine_tid())
        if not outputs: return

        od = self.get_task_path() + "outputs/"
        fs = self.environment.filescheme

        for outname, o in outputs.items():

            if isinstance(o, list):
                self.copy_sharded_outputs(fs, od, outname, o)
            elif isinstance(o, CromwellFile):
                self.copy_cromwell_output(fs, od, outname, o)
            elif isinstance(o, str):
                self.copy_output(fs, od, outname, o)

            else:
                raise Exception(f"Don't know how to handle output with type: {type(o)}")

        self.database.progress_mark_completed(ProgressKeys.copiedOutputs)

    def wait_if_required(self):

        if self.database.progress_has_completed(ProgressKeys.workflowMovedToFinalState):
            return Logger.log(f"Workflow '{self.tid}' has already finished, skipping")

        status = None

        while status not in TaskStatus.FINAL_STATES():
            meta = self.metadata()
            call('clear')
            if meta:
                print(meta.format())
                status = meta.status
            if status not in TaskStatus.FINAL_STATES():
                time.sleep(5)

        self.database.progress_mark_completed(ProgressKeys.workflowMovedToFinalState)

    @staticmethod
    def copy_output(filescheme: FileScheme, output_dir, filename, source):

        if isinstance(source, list):
            TaskManager.copy_sharded_outputs(filescheme, output_dir, filename, source)
        elif isinstance(source, CromwellFile):
            TaskManager.copy_cromwell_output(filescheme, output_dir, filename, source)
        elif isinstance(source, str):
            ext = get_extension(source)
            filescheme.cp_from(source, output_dir + filename + "." + ext, None)

    @staticmethod
    def copy_sharded_outputs(filescheme: FileScheme, output_dir, filename, outputs: List[CromwellFile]):
        pre = "shard-"
        for counter in range(len(outputs)):
            TaskManager.copy_output(filescheme, output_dir, filename + pre + str(counter), outputs[counter])

    @staticmethod
    def copy_cromwell_output(filescheme: FileScheme, output_dir: str, filename: str, out: CromwellFile):
        TaskManager.copy_output(filescheme, output_dir, filename, out.location)
        if out.secondary_files:
            for sec in out.secondary_files:
                TaskManager.copy_output(filescheme, output_dir, filename, sec.location)

    def get_engine_tid(self):
        if not self._engine_tid:
            self._engine_tid = self.database.get_meta_info(InfoKeys.engine_tid)
        return self._engine_tid

    def get_task_path(self):
        return TaskManager.get_task_path_by(self.tid)

    @staticmethod
    def get_task_path_by(tid):
        return get_default_export_dir() + tid + "/"


    def get_task_path_safe(self):
        path = self.get_task_path()
        TaskManager._create_dir_if_needed(path)
        return path

    def create_output_structure(self):
        outputdir = self.get_task_path_safe()
        folders = ["workflow", "metadata", "validation", "outputs"]

        # workflow folder
        for f in folders:
            self._create_dir_if_needed(outputdir + f)

    def watch(self):
        import time
        from subprocess import call
        status = None

        while status not in TaskStatus.FINAL_STATES():
            meta = self.metadata()
            if meta:
                call('clear')

                print(meta.format())
                status = meta.status
            if status not in TaskStatus.FINAL_STATES():
                time.sleep(2)

    def metadata(self) -> TaskMetadata:
        meta = self.environment.engine.metadata(self.get_engine_tid())
        meta.tid = self.tid
        return meta

    @staticmethod
    def _create_dir_if_needed(path):
        if not os.path.exists(path):
            os.makedirs(path)
