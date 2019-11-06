"""
I think this is where the bread and butter is!

Run / monitor separation:

    Now when you "run", the "fromjanis" function will be called and the Janis
    execution directory is set up, and all the metadata is set.

    From there, a new process is spawned off (customisable through the template),
    which starts the engine, submits the workflow and monitors it.
    We'll add a try:catch: to the whole monitor to ensure that we don't leave
    any processing lying around.



"""
import os
import time
from enum import Enum
from subprocess import call
from typing import Optional, List, Dict, Union, Any

from janis_core import InputSelector, Logger, Workflow, File, Array, LogLevel
from janis_core.translations import get_translator, CwlTranslator
from janis_core.translations.translationbase import TranslatorBase
from janis_core.translations.wdl import apply_secondary_file_format_to_filename
from janis_runner.management.configuration import JanisConfiguration

from janis_runner.data.models.filescheme import FileScheme, LocalFileScheme
from janis_runner.data.enums import TaskStatus
from janis_runner.data.models.outputs import WorkflowOutputModel
from janis_runner.data.models.workflow import WorkflowModel
from janis_runner.data.models.workflowjob import WorkflowJobModel
from janis_runner.engines import (
    get_ideal_specification_for_engine,
    Cromwell,
    CWLTool,
    EngineType,
    CromwellConfiguration,
)
from janis_runner.environments.environment import Environment
from janis_runner.management.mysql import MySql
from janis_runner.management.notificationmanager import NotificationManager
from janis_runner.management.workflowdbmanager import WorkflowDbManager
from janis_runner.utils import get_extension, recursively_join, find_free_port
from janis_runner.utils.dateutil import DateUtil
from janis_runner.validation import (
    generate_validation_workflow_from_janis,
    ValidationRequirements,
)


class WorkflowManager:

    MAX_ENGINE_ATTEMPTS = 5

    class WorkflowManagerPath(Enum):
        execution = "execution"
        workflow = "workflow"
        metadata = "metadata"
        logs = "logs"
        configuration = "configuration"
        database = "database"
        mysql = "mysql"

    def __init__(self, outdir: str, wid: str, environment: Environment = None):
        # do stuff here
        self.wid = wid

        self._failed_engine_attempts = None

        # hydrate from here if required
        self._engine_wid = None
        self.path = outdir
        self.create_dir_structure(self.path)

        self.database = WorkflowDbManager(self.get_task_path_safe())
        self.environment = environment
        self.dbcontainer: MySql = None

        self._prev_status = None

        if not self.wid:
            self.wid = self.get_engine_wid()

    @staticmethod
    def has(
        outdir: str,
        status: Optional[TaskStatus],
        name: Optional[str] = None,
        environment: Optional[str] = None,
    ):
        metadb = WorkflowDbManager.get_workflow_metadatadb(outdir)

        has = True
        if has and status:
            has = metadb.status == status
        if has and name:
            has = metadb.name.lower().startswith(name.lower())

        return metadb if has else False

    def watch(self):
        self.poll_stored_metadata()

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
        max_cores=None,
        max_memory=None,
        keep_intermediate_files=False,
        should_disconnect=True,
        skip_mysql=False,
    ):

        jc = JanisConfiguration.manager()

        # output directory has been created

        environment.identifier += "_" + wid

        tm = WorkflowManager(wid=wid, outdir=outdir, environment=environment)

        tm.database.workflowmetadata.wid = wid
        tm.database.workflowmetadata.engine = environment.engine
        tm.database.workflowmetadata.filescheme = environment.filescheme
        tm.database.workflowmetadata.environment = environment.id()
        tm.database.workflowmetadata.name = wf.id()
        tm.database.workflowmetadata.start = DateUtil.now()
        tm.database.workflowmetadata.executiondir = None
        tm.database.workflowmetadata.keepexecutiondir = keep_intermediate_files
        tm.database.workflowmetadata.configuration = jc
        tm.database.workflowmetadata.should_manage_database = not skip_mysql

        # This is the only time we're allowed to skip the tm.set_status
        # This is a temporary stop gap until "notification on status" is implemented.
        # tm.set_status(TaskStatus.PROCESSING)
        tm.database.workflowmetadata.status = TaskStatus.PROCESSING

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

        outdir_workflow = tm.get_path_for_component(
            WorkflowManager.WorkflowManagerPath.workflow
        )

        tm.database.workflowmetadata.submission_workflow = os.path.join(
            outdir_workflow, spec_translator.workflow_filename(wf_evaluate)
        )
        tm.database.workflowmetadata.submission_inputs = os.path.join(
            outdir_workflow, spec_translator.inputs_filename(wf_evaluate)
        )
        tm.database.workflowmetadata.submission_resources = os.path.join(
            outdir_workflow, spec_translator.dependencies_filename(wf_evaluate)
        )

        tm.database.commit()

        if not dryrun:
            # this happens for all workflows no matter what type
            tm.set_status(TaskStatus.QUEUED)

            # resubmit the engine
            if should_disconnect:
                loglevel = LogLevel.get_str(Logger.CONSOLE_LEVEL)
                command = ["janis", "--logLevel", loglevel, "resume", wid]
                jc.template.template.submit_detatched_resume(wid, command)
                Logger.log("Submitted detatched engine")

                if watch:
                    Logger.log("Watching submitted workflow")
                    tm.poll_stored_metadata()
            else:
                tm.resume()

        else:
            tm.set_status(TaskStatus.DRY_RUN)

        tm.database.commit()

        return tm

    @staticmethod
    def from_path(path):
        """
        :param config_manager:
        :param path: Path should include the $wid if relevant
        :return: TaskManager after resuming (might include a wait)
        """
        # get everything and pass to constructor
        # database path

        path = WorkflowManager.get_task_path_for(path)
        db = WorkflowDbManager.get_workflow_metadatadb(path)

        wid = db.wid  # .get_meta_info(InfoKeys.taskId)
        envid = db.environment  # .get_meta_info(InfoKeys.environment)
        eng = db.engine
        fs = db.filescheme
        env = Environment(envid, eng, fs)

        JanisConfiguration._managed = db.configuration

        db.close()

        tm = WorkflowManager(outdir=path, wid=wid, environment=env)
        return tm

    def poll_stored_metadata(self, seconds=3):
        """
        This function just polls the database for metadata every so often,
        and simply displays it. It will keep doing that until the task
        moves into a TERMINAL status.

        It's presumed that there's a janis-monitor that's watching the engine
        """

        if self.database.progressDB.workflowMovedToFinalState:
            meta = self.database.get_metadata()
            print(meta.format())

            return Logger.log(f"Workflow '{self.wid}' has already finished, skipping")

        status = None

        while status not in TaskStatus.final_states():
            meta = self.database.get_metadata()
            if meta:
                call("clear")
                print(meta.format())
                status = meta.status

            if status not in TaskStatus.final_states():
                time.sleep(seconds)

    def resume(self):
        """
        Resume takes an initialised DB, looks for the engine (if it's around),
        or starts it and monitors the status, updating the DB every so often
        :return:
        """

        # get a logfile and start doing stuff
        try:
            logsdir = self.get_path_for_component(self.WorkflowManagerPath.logs)

            Logger.set_write_location(os.path.join(logsdir, "janis-monitor.log"))

            self.start_engine_if_required()

            if self.database.workflowmetadata.please_abort:
                Logger.info("Detected please_abort request, aborting")
                return self.abort()

            # check status and see if we can resume
            self.submit_workflow_if_required()

            self.database.commit()
            self.watch_engine()
            Logger.info(
                f"Task has finished with status: {self.database.workflowmetadata.status}"
            )

            if self.database.workflowmetadata.status == TaskStatus.COMPLETED:
                self.save_metadata_if_required()
                self.copy_outputs_if_required()
                self.remove_exec_dir()

            self.database.workflowmetadata.engine.stop_engine()
            if self.dbcontainer:
                self.dbcontainer.stop()

            Logger.info(f"Finished managing task '{self.wid}'.")

        except Exception as e:
            import traceback

            err = traceback.format_exc()
            Logger.critical(
                f"A fatal error occurred while monitoring workflow = '{self.wid}', exiting: "
                + err
            )

            try:
                self.database.workflowmetadata.status = TaskStatus.FAILED
                self.database.workflowmetadata.error = traceback.format_exc()
                self.database.commit()

                self.database.workflowmetadata.engine.stop_engine()
                if self.dbcontainer:
                    self.dbcontainer.stop()

                self.database.close()
            except Exception as e:
                Logger.critical(
                    "An additional fatal error occurred while trying to store Janis state: "
                    + str(e)
                    + "\n\nSee the logfile for more information: "
                    + Logger.WRITE_LOCATION
                )

        Logger.close_file()

        return self

    def start_engine_if_required(self):
        # engine should be loaded from the DB
        engine = self.database.workflowmetadata.engine
        self.environment.engine = engine

        is_allegedly_started = engine.test_connection()

        if not is_allegedly_started:

            was_manually_started = False

            # start mysql if it's cromwell, and then slightly change the config if one exists
            if isinstance(engine, Cromwell):
                if engine.config:
                    if self.database.workflowmetadata.should_manage_database and (
                        self.database.workflowmetadata.manages_database
                        or not engine.config.database
                    ):
                        # only here do we start mysql
                        self.start_mysql_and_prepare_cromwell(engine)
                        self.database.workflowmetadata.manages_database = True
                        was_manually_started = True
                    else:
                        Logger.info(
                            "Skipping start mysql as database configuration already exists"
                        )

                else:
                    Logger.info(
                        "Skipping start mysql as Janis is not managing the config"
                    )

            if not was_manually_started:
                engine.start_engine()
            # Write the new engine details back into the database (for like PID, host and is_started)
            self.database.workflowmetadata.engine = engine

    def start_mysql_and_prepare_cromwell(self, engine):
        scriptsdir = self.get_path_for_component(self.WorkflowManagerPath.mysql)

        self.dbcontainer = MySql(
            wid=self.wid,
            container=JanisConfiguration.manager().container,
            datadirectory=self.get_path_for_component(
                self.WorkflowManagerPath.database
            ),
            confdir=scriptsdir,
            forwardedport=find_free_port(),
        )
        self.dbcontainer.prepare_startup_scripts_dir()
        self.dbcontainer.start()

        engine.config.database = CromwellConfiguration.Database.mysql(username="root")
        engine.config.call_caching = CromwellConfiguration.CallCaching(enabled=True)

        port = self.dbcontainer.forwardedport
        url = CromwellConfiguration.Database.MYSQL_URL.format(
            url=f"127.0.0.1:{port}", database="cromwell"
        )
        engine.start_engine(additional_cromwell_options=["-Ddatabase.db.url=" + url])

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

        outdir_workflow = self.get_path_for_component(self.WorkflowManagerPath.workflow)
        translator.translate(
            workflow,
            to_console=False,
            to_disk=True,
            with_resource_overrides=True,
            merge_resources=True,
            hints=hints,
            write_inputs_file=True,
            export_path=outdir_workflow,
            additional_inputs=additional_inputs,
            max_cores=max_cores,
            max_mem=max_memory,
        )

        Logger.log(f"Saved workflow with id '{workflow.id()}' to '{outdir_workflow}'")

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
                export_path=outdir_workflow,
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
            ext = None
            innertype = o.datatype
            while isinstance(innertype, Array):
                innertype = innertype.subtype()
            if isinstance(o.datatype, File):
                ext = o.datatype.extension
            outputs.append(
                WorkflowOutputModel(
                    tag=o.id(),
                    original_path=None,
                    new_path=None,
                    timestamp=None,
                    prefix=self.evaluate_output_selector(o.output_prefix, mapped_inps),
                    tags=self.evaluate_output_selector(o.output_tag, mapped_inps),
                    secondaries=o.datatype.secondary_files(),
                    extension=ext,
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

    def submit_workflow_if_required(self):
        if self.database.progressDB.submitWorkflow:
            return Logger.log(f"Workflow '{self.wid}' has submitted, skipping")

        fn_wf = self.database.workflowmetadata.submission_workflow
        fn_inp = self.database.workflowmetadata.submission_inputs
        fn_deps = self.database.workflowmetadata.submission_resources

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

        metadir = self.get_path_for_component(self.WorkflowManagerPath.metadata)
        if isinstance(self.environment.engine, Cromwell):
            import json

            meta = self.environment.engine.raw_metadata(self.get_engine_wid()).meta
            with open(os.path.join(metadir, "metadata.json"), "w+") as fp:
                json.dump(meta, fp)

        elif isinstance(self.environment.engine, CWLTool):
            import json

            meta = self.environment.engine.metadata(self.wid)
            self.set_status(meta.status)
            with open(os.path.join(metadir, "metadata.json"), "w+") as fp:
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
            originalfile, newfilepath = self.copy_output(
                fs=fs,
                outputid=out.tag,
                prefix=out.prefix,
                tag=out.tags,
                secondaries=out.secondaries,
                extension=out.extension,
                engine_output=eout,
            )

            if isinstance(originalfile, list):
                originalfile = recursively_join(originalfile, "|")

            if isinstance(newfilepath, list):
                newfilepath = recursively_join(newfilepath, "|")

            self.database.outputsDB.update_paths(
                tag=out.tag, original_path=originalfile, new_path=newfilepath
            )

        self.database.progressDB.copiedOutputs = True
        Logger.info(f"View the task outputs: file://{self.get_task_path()}")

    def copy_output(
        self,
        fs: FileScheme,
        outputid,
        prefix,
        tag,
        secondaries,
        extension,
        engine_output: Union[WorkflowOutputModel, Any, List[Any]],
        shard=None,
    ):

        # the output_tag is an array of an array, for each

        if isinstance(engine_output, list):
            outs = []
            s = 0
            nshards = len(engine_output)
            prev_shards = shard or []

            # This is a little complicated, we want to output the set of tags that have the same length as we
            # we have shards. We'll only let this work if there's one element in the array with the appropriate amount
            # of shards.

            # find the index

            def find_element_where_length_is(iterable, n):
                if iterable is None:
                    return None
                for i in range(len(iterable)):
                    ii = iterable[i]
                    if isinstance(ii, list) and len(ii) == n:
                        return i
                return None

            def explode_at_index(iterable, index_to_explode, index_to_select):
                ar = iterable[:index_to_explode] + [
                    iterable[index_to_explode][index_to_select]
                ]
                if index_to_explode + 1 < len(iterable):
                    ar.extend(iterable[1 + tag_index_to_explode :])
                return ar

            tag_index_to_explode = find_element_where_length_is(tag, nshards)

            for i in range(nshards):
                eout = engine_output[i]

                # choose tag
                new_prefix = (
                    prefix[i]
                    if (isinstance(prefix, list) and len(prefix) > 1)
                    else prefix
                )

                new_tag = tag
                if tag_index_to_explode is not None:
                    new_tag = explode_at_index(tag, tag_index_to_explode, i)

                outs.append(
                    self.copy_output(
                        fs,
                        outputid=outputid,
                        tag=new_tag,
                        prefix=new_prefix,
                        engine_output=eout,
                        shard=[*prev_shards, s],
                        secondaries=secondaries,
                        extension=extension,
                    )
                )
                s += 1
            return [o[0] for o in outs], [o[1] for o in outs]

        final_tags = tag
        final_prefix = prefix

        if final_tags and any(isinstance(t, list) for t in final_tags):
            Logger.critical(
                f"One of the final output tags {str(final_tags)} was still an array. This is an issue, "
                f"so we're going to default to the generic 'output' directory"
            )
            final_tags = None

        if isinstance(final_prefix, list):
            if len(final_prefix) > 1:
                final_prefix = None
            else:
                if len(final_prefix) > 1:
                    Logger.critical(
                        f"Expected only one prefix for this copy, but found ({', '.join(final_prefix)}) [{len(final_prefix)}], using the first"
                    )
                final_prefix = final_prefix[0]

        if not final_tags:
            final_tags = ["output"]

        outdir = os.path.join(self.path, "/".join(final_tags))

        fs.mkdirs(outdir)

        pr = (final_prefix + "_") if final_prefix else ""
        outfn = pr + outputid

        if shard is not None:
            for s in shard:
                outfn += f"_shard-{s}"

        # copy output

        original_filepath = None
        newoutputfilepath = os.path.join(outdir, outfn)

        if isinstance(engine_output, WorkflowOutputModel):
            original_filepath = engine_output.originalpath
            ext = extension or get_extension(engine_output.originalpath)
            if ext:
                dot = "" if ext[0] == "." else "."
                outfn += dot + ext
                newoutputfilepath += dot + ext
            fs.cp_from(engine_output.originalpath, newoutputfilepath, None)
        else:
            original_filepath = engine_output
            if isinstance(fs, LocalFileScheme):
                # Write engine_output to outpath
                with open(newoutputfilepath, "w+") as outfile:
                    outfile.write(engine_output)

        for sec in secondaries or []:
            frompath = apply_secondary_file_format_to_filename(original_filepath, sec)
            tofn = apply_secondary_file_format_to_filename(outfn, sec)
            topath = os.path.join(outdir, tofn)
            fs.cp_from(frompath, topath, None)

        return [original_filepath, newoutputfilepath]

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

    def get_path_for_component(self, component: WorkflowManagerPath):
        return self.get_path_for_component_and_dir(self.get_task_path_safe(), component)

    @staticmethod
    def get_path_for_component_and_dir(path, component: WorkflowManagerPath):
        val = os.path.join(path, "janis", component.value)
        if not os.path.exists(val):
            os.makedirs(val, exist_ok=True)
        return val

    @staticmethod
    def create_dir_structure(path):
        outputdir = WorkflowManager.get_task_path_for(path)
        WorkflowManager._create_dir_if_needed(path)

        # workflow folder
        for comp in WorkflowManager.WorkflowManagerPath:
            WorkflowManager._create_dir_if_needed(
                WorkflowManager.get_path_for_component_and_dir(outputdir, comp)
            )

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

    def watch_engine(self):
        import time

        status = None

        while status not in TaskStatus.final_states() and (
            self._failed_engine_attempts is None
            or self._failed_engine_attempts <= self.MAX_ENGINE_ATTEMPTS
        ):
            meta = self.save_metadata()
            if meta:
                Logger.log("Got metadata from engine")
                status = meta.status
                self.set_status(status)
                self._failed_engine_attempts = 0
            elif self._failed_engine_attempts is not None:
                # We'll only add a failed attempt if we've contacted it before
                self._failed_engine_attempts += 1

            if status not in TaskStatus.final_states():
                time.sleep(5)

            if self.database.workflowmetadata.please_abort:
                Logger.info("Detected please_abort request during poll, aborting")
                status = self.abort()

            if (
                self._failed_engine_attempts
                and self._failed_engine_attempts >= self.MAX_ENGINE_ATTEMPTS
            ):
                Logger.critical(
                    f"Failed to contact the engine at least {self.MAX_ENGINE_ATTEMPTS}, "
                    f"you might need to restart janis by: `janis resume {self.wid}`"
                )

    def set_status(self, status: TaskStatus, force_notification=False):
        prev = self.database.workflowmetadata.status

        if prev == status and not force_notification:
            return
        Logger.log("Status changed to: " + str(status))
        self.database.workflowmetadata.status = status
        self.database.commit()
        # send an email here

        NotificationManager.notify_status_change(status, self.database.get_metadata())

    def save_metadata(self) -> Optional[WorkflowModel]:
        meta = self.environment.engine.metadata(self.get_engine_wid())

        if not meta:
            return None

        self.database.save_metadata(meta)

        self.set_status(meta.status)

        if meta.execution_dir:
            self.database.workflowmetadata.execution_dir = meta.execution_dir

        if meta.finish:
            self.database.workflowmetadata.finish = meta.finish

        return meta

    @staticmethod
    def mark_aborted(outputdir) -> bool:
        try:
            db = WorkflowDbManager.get_workflow_metadatadb(outputdir)
            db.please_abort = True
            db.kvdb.commit()
            Logger.info("Marked workflow as aborted, this may take some time full exit")
            return True
        except Exception as e:
            Logger.critical("Couldn't mark aborted: " + str(e))
            return False

    def abort(self) -> bool:
        self.set_status(TaskStatus.ABORTED, force_notification=True)
        status = False
        try:
            status = bool(self.environment.engine.terminate_task(self.get_engine_wid()))
        except Exception as e:
            Logger.critical("Couldn't abort task from engine: " + str(e))
        try:
            self.environment.engine.stop_engine()
            if self.dbcontainer:
                self.dbcontainer.stop()
        except Exception as e:
            Logger.critical("Couldn't stop engine: " + str(e))

        return status

    @staticmethod
    def _create_dir_if_needed(path):
        if not os.path.exists(path):
            os.makedirs(path)
