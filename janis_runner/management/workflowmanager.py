"""
I think this is where the bread and butter is!

A task manager will have reference to a database,


"""
import os
import time
from subprocess import call
from typing import Optional, List, Dict, Union, Any

from janis_core import InputSelector, Logger, Workflow
from janis_core.translations import get_translator, CwlTranslator
from janis_core.translations.translationbase import TranslatorBase
from janis_core.translations.wdl import apply_secondary_file_format_to_filename

from janis_runner.data.models.filescheme import FileScheme, LocalFileScheme
from janis_runner.data.enums import TaskStatus
from janis_runner.data.models.outputs import WorkflowOutputModel
from janis_runner.data.models.workflow import WorkflowModel
from janis_runner.data.models.workflowjob import WorkflowJobModel
from janis_runner.engines import get_ideal_specification_for_engine, Cromwell, CWLTool
from janis_runner.environments.environment import Environment
from janis_runner.management.workflowdbmanager import WorkflowDbManager
from janis_runner.utils import get_extension
from janis_runner.utils.dateutil import DateUtil
from janis_runner.validation import (
    generate_validation_workflow_from_janis,
    ValidationRequirements,
)


class WorkflowManager:
    def __init__(self, outdir: str, wid: str, environment: Environment = None):
        # do stuff here
        self.wid = wid

        # hydrate from here if required
        self._engine_wid = None
        self.path = outdir
        self.outdir_workflow = (
            self.get_task_path() + "workflow/"
        )  # handy to have as reference
        self.create_dir_structure(self.path)

        self.database = WorkflowDbManager(self.get_task_path_safe())
        self.environment = environment

        if not self.wid:
            self.wid = self.get_engine_wid()

    def has(
        self,
        status: Optional[TaskStatus],
        name: Optional[str] = None,
        environment: Optional[str] = None,
    ):
        has = True
        if status:
            has = has and self.database.workflowmetadata.status == status
        if name:
            has = has and self.database.workflowmetadata.name.lower().startswith(
                name.lower()
            )
        return has

    @staticmethod
    def from_janis(
        wid: str,
        outdir: str,
        wf: Workflow,
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
    ):

        # output directory has been created

        environment.identifier += "_" + wid

        tm = WorkflowManager(wid=wid, outdir=outdir, environment=environment)

        tm.database.workflowmetadata.wid = wid
        tm.database.workflowmetadata.status = TaskStatus.PROCESSING
        tm.database.workflowmetadata.engine = environment.engine
        tm.database.workflowmetadata.filescheme = environment.filescheme
        tm.database.workflowmetadata.environment = environment.id()
        tm.database.workflowmetadata.name = wf.id()
        tm.database.workflowmetadata.start = DateUtil.now()
        tm.database.workflowmetadata.executiondir = None
        tm.database.workflowmetadata.keepexecutiondir = keep_intermediate_files

        spec = get_ideal_specification_for_engine(environment.engine)
        spec_translator = get_translator(spec)
        wf_evaluate = tm.prepare_and_output_workflow_to_evaluate_if_required(
            workflow=wf,
            translator=spec_translator,
            validation=validation_requirements,
            hints=hints,
            additional_inputs=inputs_dict,
            max_cores=max_cores,
            max_memory=max_memory,
        )

        if not dryrun:
            # this happens for all workflows no matter what type
            tm.database.workflowmetadata.status = TaskStatus.QUEUED
            tm.submit_workflow_if_required(wf_evaluate, spec_translator)
            if watch:
                tm.resume_if_possible(show_metadata=show_metadata)
        else:
            tm.database.workflowmetadata.status = "DRY-RUN"

        return tm

    @staticmethod
    def from_path(path, config_manager):
        """
        :param config_manager:
        :param path: Path should include the $wid if relevant
        :return: TaskManager after resuming (might include a wait)
        """
        # get everything and pass to constructor
        # database path

        path = WorkflowManager.get_task_path_for(path)
        db = WorkflowDbManager(path)

        wid = db.workflowmetadata.wid  # .get_meta_info(InfoKeys.taskId)
        envid = db.workflowmetadata.environment  # .get_meta_info(InfoKeys.environment)
        eng = db.workflowmetadata.engine
        fs = db.workflowmetadata.filesystem
        env = Environment(envid, eng, fs)

        db.close()

        tm = WorkflowManager(outdir=path, wid=wid, environment=env)
        return tm

    def resume_if_possible(self, show_metadata=True):
        # check status and see if we can resume
        if not self.database.progressDB.submitWorkflow:
            return Logger.critical(
                f"Can't resume workflow with id '{self.wid}' as the workflow "
                "was not submitted to the engine"
            )
        self.wait_if_required(show_metadata=show_metadata)
        self.save_metadata_if_required()
        self.copy_outputs_if_required()
        self.remove_exec_dir()
        self.environment.engine.stop_engine()

        print(
            f"Finished managing task '{self.wid}'. View the task outputs: file://{self.get_task_path()}"
        )
        return self

    def prepare_and_output_workflow_to_evaluate_if_required(
        self,
        workflow,
        translator: TranslatorBase,
        validation: ValidationRequirements,
        hints: Dict[str, str],
        additional_inputs: dict,
        max_cores=None,
        max_memory=None,
    ):
        if self.database.progressDB.saveWorkflow:
            return Logger.info(f"Saved workflow from task '{self.wid}', skipping.")

        Logger.log(f"Saving workflow with id '{workflow.id()}'")

        translator.translate(
            workflow,
            to_console=False,
            to_disk=True,
            with_resource_overrides=True,
            merge_resources=True,
            hints=hints,
            write_inputs_file=True,
            export_path=self.outdir_workflow,
            additional_inputs=additional_inputs,
            max_cores=max_cores,
            max_mem=max_memory,
        )

        Logger.log(
            f"Saved workflow with id '{workflow.id()}' to '{self.outdir_workflow}'"
        )

        workflow_to_evaluate = workflow
        if validation:
            Logger.log(
                f"Validation requirements provided, wrapping workflow '{workflow.id()}' with hap.py"
            )

            # we need to generate both the validation and non-validation workflow
            workflow_to_evaluate = generate_validation_workflow_from_janis(
                workflow, validation
            )

            wid = workflow.id()

            adjusted_inputs = (
                {wid + "_" + k: v for k, v in additional_inputs.items()}
                if additional_inputs
                else None
            )

            translator.translate(
                workflow_to_evaluate,
                to_console=False,
                to_disk=True,
                with_resource_overrides=True,
                merge_resources=True,
                hints=hints,
                write_inputs_file=True,
                export_path=self.outdir_workflow,
                additional_inputs=adjusted_inputs,
                max_cores=max_cores,
                max_mem=max_memory,
            )

        self.evaluate_output_params(wf=workflow, additional_inputs=additional_inputs)

        self.database.progressDB.saveWorkflow = True
        return workflow_to_evaluate

    def evaluate_output_params(self, wf: Workflow, additional_inputs: dict):
        mapped_inps = CwlTranslator().build_inputs_file(
            wf, recursive=False, additional_inputs=additional_inputs
        )

        outputs: List[WorkflowOutputModel] = []

        for o in wf.output_nodes.values():
            # We'll
            outputs.append(
                WorkflowOutputModel(
                    tag=o.id(),
                    original_path=None,
                    new_path=None,
                    timestamp=None,
                    prefix=self.evaluate_output_selector(o.output_prefix, mapped_inps),
                    tags=self.evaluate_output_selector(o.output_tag, mapped_inps),
                    secondaries=o.datatype.secondary_files(),
                )
            )

        return self.database.outputsDB.insert_many(outputs)

    def evaluate_output_selector(self, selector, inputs: dict):
        if selector is None:
            return None

        if isinstance(selector, str):
            return selector

        if isinstance(selector, list):
            return [self.evaluate_output_selector(s, inputs) for s in selector]

        if isinstance(selector, InputSelector):
            if selector.input_to_select not in inputs:
                Logger.warn(f"Couldn't find the input {selector.input_to_select}")
                return None
            return inputs[selector.input_to_select]

        raise Exception(
            f"Janis runner cannot evaluate selecting the output from a {type(selector).__name__} type"
        )

    def submit_workflow_if_required(self, wf, translator):
        if self.database.progressDB.submitWorkflow:
            return Logger.log(f"Workflow '{self.wid}' has submitted finished, skipping")

        fn_wf = self.outdir_workflow + translator.workflow_filename(wf)
        fn_inp = self.outdir_workflow + translator.inputs_filename(wf)
        fn_deps = self.outdir_workflow + translator.dependencies_filename(wf)

        Logger.log(f"Submitting task '{self.wid}' to '{self.environment.engine.id()}'")
        self._engine_wid = self.environment.engine.start_from_paths(
            self.wid, fn_wf, fn_inp, fn_deps
        )
        self.database.workflowmetadata.engine_wid = self._engine_wid
        Logger.log(
            f"Submitted workflow ({self.wid}), got engine id = '{self.get_engine_wid()}'"
        )
        self.database.progressDB.submitWorkflow = True

    def save_metadata_if_required(self):
        if self.database.progressDB.savedMetadata:
            return Logger.log(f"Workflow '{self.wid}' has saved metadata, skipping")

        if isinstance(self.environment.engine, Cromwell):
            import json

            meta = self.environment.engine.raw_metadata(self.get_engine_wid()).meta
            with open(self.get_task_path() + "metadata/metadata.json", "w+") as fp:
                json.dump(meta, fp)

        elif isinstance(self.environment.engine, CWLTool):
            import json

            meta = self.environment.engine.metadata(self.wid)
            self.database.workflowmetadata.status = meta.status
            with open(self.get_task_path() + "metadata/metadata.json", "w+") as fp:
                json.dump(meta.outputs, fp)

        else:
            raise Exception(
                f"Don't know how to save metadata for engine '{self.environment.engine.id()}'"
            )

        self.database.progressDB.savedMetadata = True

    def copy_outputs_if_required(self):
        if self.database.progressDB.copiedOutputs:
            return Logger.log(f"Workflow '{self.wid}' has copied outputs, skipping")

        if self.database.workflowmetadata.status != TaskStatus.COMPLETED:
            return Logger.warn(
                f"Skipping copying outputs as workflow "
                f"status was not completed ({self.database.workflowmetadata.status})"
            )

        wf_outputs = self.database.outputsDB.get_all()
        engine_outputs = self.environment.engine.outputs_task(self.get_engine_wid())
        eoutkeys = engine_outputs.keys()
        fs = self.environment.filescheme

        for out in wf_outputs:
            eout = engine_outputs.get(out.tag)

            if eout is None:
                Logger.warn(
                    f"Couldn't find expected output with tag {out.tag}, found outputs ({', '.join(eoutkeys)}"
                )
                continue
            originalfile, newfilepath = self.copy_output(fs, out, eout)

            if isinstance(originalfile, list):
                originalfile = "|".join(originalfile)

            if isinstance(newfilepath, list):
                newfilepath = "|".join(newfilepath)

            self.database.outputsDB.update_paths(
                tag=out.tag, original_path=originalfile, new_path=newfilepath
            )

        self.database.progressDB.copiedOutputs = True

    def copy_output(
        self,
        fs: FileScheme,
        out: WorkflowOutputModel,
        engine_output: Union[WorkflowOutputModel, Any, List[Any]],
        shard=None,
    ):

        if isinstance(engine_output, list):
            outs = []
            s = 0
            prev_shards = shard or []
            for eout in engine_output:
                outs.append(self.copy_output(fs, out, eout, shard=[*prev_shards, s]))
                s += 1
            return [o[0] for o in outs], [o[1] for o in outs]

        merged_tags = "/".join(out.tags or ["outputs"])
        outdir = os.path.join(self.path, merged_tags)

        fs.mkdirs(outdir)

        prefix = (out.prefix + "_") if out.prefix else ""
        outfn = prefix + out.tag

        if shard is not None:
            for s in shard:
                outfn += f"_shard-{s}"

        # copy output

        original_filepath = None
        newoutputfilepath = os.path.join(outdir, outfn)

        if isinstance(engine_output, WorkflowOutputModel):
            original_filepath = engine_output.originalpath
            ext = get_extension(engine_output.originalpath)
            if ext:
                outfn += ext
                newoutputfilepath += "." + ext
            fs.cp_from(engine_output.originalpath, newoutputfilepath, None)
        else:
            if isinstance(fs, LocalFileScheme):
                # Write engine_output to outpath
                original_filepath = engine_output
                with open(newoutputfilepath, "w+") as outfile:
                    outfile.write(engine_output)

        for sec in out.secondaries or []:
            frompath = apply_secondary_file_format_to_filename(
                engine_output.originalpath, sec
            )
            tofn = apply_secondary_file_format_to_filename(outfn, sec)
            topath = os.path.join(outdir, tofn)
            fs.cp_from(frompath, topath, None)

        return [original_filepath, newoutputfilepath]

    def wait_if_required(self, show_metadata=True):

        if self.database.progressDB.workflowMovedToFinalState:
            meta = self.database.get_metadata()
            print(meta.format())

            return Logger.log(f"Workflow '{self.wid}' has already finished, skipping")

        status = None

        while status not in TaskStatus.final_states():
            self.save_metadata()
            meta = self.database.get_metadata()
            if meta:
                if show_metadata:
                    call("clear")
                    print(meta.format())
                status = meta.status

            if status not in TaskStatus.final_states():
                if not show_metadata:
                    time.sleep(5)
                else:
                    for _ in range(2):
                        time.sleep(3)
                        call("clear")
                        print(meta.format())

        self.database.workflowmetadata.finish = DateUtil.now()
        self.database.progressDB.workflowMovedToFinalState = True

    def remove_exec_dir(self):
        status = self.database.workflowmetadata.status

        keep_intermediate = self.database.workflowmetadata.keepexecutiondir
        if (
            not keep_intermediate
            and status is not None
            and status == str(TaskStatus.COMPLETED)
        ):
            execdir = self.database.workflowmetadata.execution_dir
            if execdir and execdir != "None":
                Logger.info("Cleaning up execution directory")
                self.environment.filescheme.rm_dir(execdir)
                self.database.progressDB.cleanedUp = True

    def log_dbtaskinfo(self):
        import tabulate

        # log all the metadata we have:
        results = self.database.workflowmetadata.kvdb.items()
        header = ("Key", "Value")
        res = tabulate.tabulate([header, ("wid", self.wid), *results])
        print(res)
        return res

    def get_engine_wid(self):
        if not self._engine_wid:
            self._engine_wid = self.database.workflowmetadata.engine_wid
        return self._engine_wid

    def get_task_path(self):
        return WorkflowManager.get_task_path_for(self.path)

    @staticmethod
    def get_task_path_for(outdir: str):
        return outdir

    def get_task_path_safe(self):
        path = self.get_task_path()
        WorkflowManager._create_dir_if_needed(path)
        return path

    @staticmethod
    def create_dir_structure(path):
        outputdir = WorkflowManager.get_task_path_for(path)
        folders = ["workflow", "metadata", "logs", "configuration"]
        WorkflowManager._create_dir_if_needed(path)

        # workflow folder
        for f in folders:
            WorkflowManager._create_dir_if_needed(os.path.join(outputdir, f))

    def copy_logs_if_required(self):
        if not self.database.progressDB.savedLogs:
            return Logger.log(f"Workflow '{self.wid}' has copied logs, skipping")

        meta = self.save_metadata()

    @staticmethod
    def get_logs_from_jobs_meta(meta: List[WorkflowJobModel]):
        ms = []

        for j in meta:
            ms.append((f"{j.batchid}-stderr", j.stderr))
            ms.append((f"{j.batchid}-stdout", j.stderr))

    def watch(self):
        import time
        from subprocess import call

        status = None

        while status not in TaskStatus.final_states():
            meta = self.save_metadata()
            if meta:
                call("clear")
                print(meta.format())
                status = meta.status
            if status not in TaskStatus.final_states():
                time.sleep(2)

    def save_metadata(self) -> Optional[WorkflowModel]:
        meta = self.environment.engine.metadata(self.get_engine_wid())

        if not meta:
            return None

        self.database.workflowmetadata.status = meta.status
        if meta.execution_dir:
            self.database.workflowmetadata.execution_dir = meta.execution_dir

        self.database.save_metadata(meta)

        return meta

    def abort(self) -> bool:
        self.database.workflowmetadata.status = TaskStatus.ABORTED
        status = False
        try:
            status = bool(self.environment.engine.terminate_task(self.get_engine_wid()))
        except Exception as e:
            Logger.critical("Couldn't abort task from engine: " + str(e))
        try:
            self.environment.engine.stop_engine()
        except Exception as e:
            Logger.critical("Couldn't stop engine: " + str(e))

        return status

    @staticmethod
    def _create_dir_if_needed(path):
        if not os.path.exists(path):
            os.makedirs(path)
