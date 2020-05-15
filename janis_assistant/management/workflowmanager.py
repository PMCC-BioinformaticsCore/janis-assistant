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
import queue
import time
from enum import Enum
from subprocess import call
from typing import Optional, List, Dict, Union, Any

from janis_core import (
    InputSelector,
    Logger,
    Workflow,
    File,
    Array,
    LogLevel,
    Directory,
    Tool,
)
from janis_core.translations import get_translator, CwlTranslator
from janis_core.translations.translationbase import TranslatorBase
from janis_core.translations.wdl import apply_secondary_file_format_to_filename

from janis_assistant.data.enums import TaskStatus, ProgressKeys
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.workflow import WorkflowModel
from janis_assistant.engines import (
    get_ideal_specification_for_engine,
    Cromwell,
    CWLTool,
    CromwellConfiguration,
    Engine,
)
from janis_assistant.environments.environment import Environment
from janis_assistant.management.configuration import (
    JanisConfiguration,
    JanisDatabaseConfigurationHelper,
)
from janis_assistant.management.filescheme import FileScheme, LocalFileScheme
from janis_assistant.management.mysql import MySql
from janis_assistant.management.notificationmanager import NotificationManager
from janis_assistant.management.workflowdbmanager import WorkflowDbManager
from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.modifiers.batchmodifier import BatchPipelineModifier
from janis_assistant.modifiers.inputchecker import InputChecker
from janis_assistant.modifiers.inputqualifier import InputFileQualifierModifier
from janis_assistant.modifiers.validatormodifier import ValidatorPipelineModifier
from janis_assistant.templates.base import SingularityEnvironmentTemplate
from janis_assistant.utils import (
    get_extension,
    recursively_join,
    find_free_port,
    fully_qualify_filename,
)
from janis_assistant.utils.batchrun import BatchRunRequirements
from janis_assistant.utils.dateutil import DateUtil
from janis_assistant.validation import ValidationRequirements


class WorkflowManager:

    MAX_ENGINE_ATTEMPTS = 5

    class WorkflowManagerPath(Enum):
        execution = "execution"
        workflow = "workflow"
        metadata = "metadata"
        logs = "logs"
        configuration = "configuration"
        database = "database"
        mysql = "configuration/mysql"

    def __init__(
        self, outdir: str, wid: str, environment: Environment = None, readonly=False
    ):
        # do stuff here
        self.wid = wid

        self._failed_engine_attempts = None

        # hydrate from here if required
        self._engine_wid = None
        self.path = fully_qualify_filename(outdir)
        self.create_dir_structure(self.path)

        self.database = WorkflowDbManager(
            wid, self.get_task_path_safe(), readonly=readonly
        )
        self.environment = environment
        self.dbcontainer: MySql = None
        self.main_queue = queue.Queue()

        self._prev_status = None
        self._engine: Optional[Engine] = None

        if not self.wid:
            self.wid = self.get_engine_wid()

    @staticmethod
    def has(
        outdir: str,
        wid: str,
        status: Optional[TaskStatus],
        name: Optional[str] = None,
        environment: Optional[str] = None,
    ):
        metadb = WorkflowDbManager.get_workflow_metadatadb(outdir, wid)

        has = True
        if has and status:
            has = metadb.status == status
        if has and name:
            has = metadb.name.lower().startswith(name.lower())

        return metadb if has else False

    def watch(self):
        self.show_status_screen()

    @staticmethod
    def from_janis(
        wid: str,
        outdir: str,
        tool: Tool,
        environment: Environment,
        hints: Dict[str, str],
        validation_requirements: Optional[ValidationRequirements],
        batchrun_requirements: Optional[BatchRunRequirements],
        inputs_dict: dict = None,
        dryrun=False,
        watch=True,
        max_cores=None,
        max_memory=None,
        keep_intermediate_files=False,
        run_in_background=True,
        dbconfig=None,
        allow_empty_container=False,
        container_override: dict = None,
        check_files=True,
    ):

        jc = JanisConfiguration.manager()

        # output directory has been created

        environment.identifier += "_" + wid

        tm = WorkflowManager(wid=wid, outdir=outdir, environment=environment)

        tm.database.runs.insert(wid)

        tm.database.workflowmetadata.wid = wid
        tm.database.workflowmetadata.engine = environment.engine
        tm.database.workflowmetadata.filescheme = environment.filescheme
        tm.database.workflowmetadata.environment = environment.id()
        tm.database.workflowmetadata.name = tool.id()
        tm.database.workflowmetadata.start = DateUtil.now()
        tm.database.workflowmetadata.executiondir = None
        tm.database.workflowmetadata.keepexecutiondir = keep_intermediate_files
        tm.database.workflowmetadata.configuration = jc
        tm.database.workflowmetadata.dbconfig = dbconfig

        # This is the only time we're allowed to skip the tm.set_status
        # This is a temporary stop gap until "notification on status" is implemented.
        # tm.set_status(TaskStatus.PROCESSING)
        tm.database.workflowmetadata.status = TaskStatus.PROCESSING

        tm.database.commit()

        spec = get_ideal_specification_for_engine(environment.engine)
        spec_translator = get_translator(spec)
        tool_evaluate = tm.prepare_and_output_workflow_to_evaluate_if_required(
            tool=tool,
            translator=spec_translator,
            validation=validation_requirements,
            batchrun=batchrun_requirements,
            hints=hints,
            additional_inputs=inputs_dict,
            max_cores=max_cores or jc.environment.max_cores,
            max_memory=max_memory or jc.environment.max_ram,
            allow_empty_container=allow_empty_container,
            container_override=container_override,
            check_files=check_files,
        )

        outdir_workflow = tm.get_path_for_component(
            WorkflowManager.WorkflowManagerPath.workflow
        )

        tm.database.workflowmetadata.submission_workflow = os.path.join(
            outdir_workflow, spec_translator.filename(tool_evaluate)
        )
        tm.database.workflowmetadata.submission_inputs = os.path.join(
            outdir_workflow, spec_translator.inputs_filename(tool_evaluate)
        )
        tm.database.workflowmetadata.submission_resources = os.path.join(
            outdir_workflow, spec_translator.dependencies_filename(tool_evaluate)
        )

        tm.database.commit()

        if not dryrun:
            if (
                not run_in_background
                and jc.template
                and jc.template.template
                and jc.template.template.can_run_in_foreground is False
            ):
                raise Exception(
                    f"Your template '{jc.template.template.__class__.__name__}' is not allowed to run "
                    f"in the foreground, try adding the '--background' argument"
                )
            tm.start_or_submit(run_in_background=run_in_background, watch=watch)
        else:
            tm.set_status(TaskStatus.DRY_RUN)

        tm.database.commit()

        return tm

    def start_or_submit(self, run_in_background, watch=False):
        # check container environment is loaded
        metadb = self.database.workflowmetadata

        jc = metadb.configuration
        metadb.containertype = jc.container.__name__
        metadb.containerversion = jc.container.test_available_by_getting_version()

        # this happens for all workflows no matter what type
        self.set_status(TaskStatus.QUEUED)

        wid = metadb.wid

        # resubmit the engine
        if not run_in_background:
            return self.resume()

        loglevel = LogLevel.get_str(Logger.CONSOLE_LEVEL)
        command = ["janis", "--logLevel", loglevel, "resume", "--foreground", wid]
        scriptdir = self.get_path_for_component(self.WorkflowManagerPath.configuration)
        logdir = self.get_path_for_component(self.WorkflowManagerPath.logs)
        jc.template.template.submit_detatched_resume(
            wid=wid, command=command, scriptdir=scriptdir, logsdir=logdir, config=jc
        )

        Logger.info("Submitted detatched engine")

        if watch:
            Logger.log("Watching submitted workflow")
            self.show_status_screen()

    @staticmethod
    def from_path_with_wid(path, wid, readonly=False):
        """
        :param wid: Workflow ID
        :param path: Path to workflow
        :return: TaskManager after resuming (might include a wait)
        """
        # get everything and pass to constructor
        # database path

        path = WorkflowManager.get_task_path_for(path)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Couldn't find path '{path}'")

        db = WorkflowDbManager.get_workflow_metadatadb(path, wid, readonly=readonly)

        if not wid:
            wid = db.wid  # .get_meta_info(InfoKeys.taskId)

        if not wid:
            raise Exception(f"Couldn't find workflow with id '{wid}'")

        envid = db.environment  # .get_meta_info(InfoKeys.environment)
        eng = db.engine
        fs = db.filescheme
        env = Environment(envid, eng, fs)

        try:
            JanisConfiguration._managed = db.configuration
        except Exception as e:
            Logger.critical(
                "The JanisConfiguration could not be loaded from the DB, this might be due to an older version, we'll load your current config instead. Error: "
                + str(e)
            )
            JanisConfiguration.initial_configuration(None)

        db.close()

        tm = WorkflowManager(outdir=path, wid=wid, environment=env, readonly=readonly)
        return tm

    @staticmethod
    def from_path_get_latest(path, readonly=False):
        path = WorkflowManager.get_task_path_for(path)
        wid = WorkflowDbManager.get_latest_workflow(path=path)
        return WorkflowManager.from_path_with_wid(path, wid, readonly=readonly)

    def show_status_screen(self):
        """
        This function just polls the database for metadata every so often,
        and simply displays it. It will keep doing that until the task
        moves into a TERMINAL status.

        It's presumed that there's a janis-monitor that's watching the engine
        """

        if self.database.progressDB.has(ProgressKeys.workflowMovedToFinalState):
            meta = self.database.get_metadata()
            formatted = meta.format()
            print(formatted)
            return Logger.debug(f"Workflow '{self.wid}' has already finished, skipping")

        bl = None

        # try:
        #     import blessed
        #
        #     bl = blessed
        #
        # except Exception as e:
        #     txt = (
        #         "Couldn't load 'blessed' for screen display, defaulting back to clear(): "
        #         + str(e)
        #     )
        #     Logger.warn(txt)

        if bl is not None:
            self.poll_stored_metadata_with_blessed(bl)
        else:
            self.poll_stored_metadata_with_clear()

    def get_meta_call(self):
        meta = self.database.get_metadata()
        return meta, meta and meta.status in TaskStatus.final_states()

    def poll_stored_metadata_with_clear(self, seconds=3):
        is_finished = False

        # We won't clear the screen if we haven't printed (first loop) and it's finished
        has_printed = False
        while not is_finished:
            meta, is_finished = self.get_meta_call()
            if meta:
                if has_printed or not is_finished:
                    call("clear")
                print(meta.format())
                has_printed = True

            if not is_finished:
                time.sleep(seconds)

    def poll_stored_metadata_with_blessed(self, blessed, seconds=1):

        term = blessed.Terminal()
        with term.fullscreen(), term.cbreak():
            try:
                print("loading...")

                is_finished = False
                did_exit = False

                # We won't clear the screen if we haven't printed (first loop) and it's finished
                has_printed = False
                while not is_finished:
                    meta, is_finished = self.get_meta_call()
                    if meta:
                        if has_printed or not is_finished:
                            print(term.clear)
                        print(meta.format())
                        has_printed = True

                    val = term.inkey(timeout=0)
                    if val and val.lower() == "q":
                        did_exit = True
                        break

                    elif not is_finished:
                        time.sleep(seconds)

                # Finished
                if not did_exit:
                    with term.location(0, term.height - 1):
                        print("Finished, press any key to quit")
                    term.inkey(timeout=None)

            except KeyboardInterrupt:
                Logger.info("Exiting")

    def process_completed_task(self):
        Logger.info(
            f"Task has finished with status: {self.database.workflowmetadata.status}"
        )

        self.save_metadata_if_required()
        self.copy_logs_if_required()
        self.copy_outputs_if_required()

        if self.database.workflowmetadata.status == TaskStatus.COMPLETED:
            self.remove_exec_dir()

        self.get_engine().stop_engine()
        if self.dbcontainer:
            self.dbcontainer.stop()

        Logger.info(f"Finished managing task '{self.wid}'.")

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

            # in case anything relies on CD, we'll throw it into janis/execution
            os.chdir(self.get_path_for_component(self.WorkflowManagerPath.execution))

            self.start_engine_if_required()

            if self.database.workflowmetadata.please_abort:
                Logger.info("Detected please_abort request, aborting")
                return self.abort()

            if self.database.workflowmetadata.please_pause:
                Logger.info("Detecting please_pause request, exiting")
                return self.stop_computation()

            # check status and see if we can resume
            self.submit_workflow_if_required()

            self.database.commit()
            self.get_engine().add_callback(
                self.get_engine_wid(),
                lambda meta: self.main_queue.put(lambda: self.save_metadata(meta)),
            )

            # add extra check for engine on resume
            meta = self._engine.metadata(self.get_engine_wid())
            if meta and meta.status in TaskStatus.final_states():
                self.save_metadata(meta)
                return self.process_completed_task()

            while True:
                try:
                    cb = self.main_queue.get(False)
                    # callback from add_callback() returns True if in TaskStatus.final_states()
                    res = cb()
                    if res is True:
                        break
                except queue.Empty:

                    if self.database.workflowmetadata.please_abort:
                        self.abort()
                        return
                    if self.database.workflowmetadata.please_pause:
                        self.database.workflowmetadata.please_pause = False
                        return self.stop_computation()

                    continue

            self.process_completed_task()

        except Exception as e:
            import traceback

            err = traceback.format_exc()
            Logger.critical(
                f"A fatal error occurred while monitoring workflow = '{self.wid}', exiting: {e}: {err}"
            )

            try:
                self.database.workflowmetadata.status = TaskStatus.FAILED
                self.database.workflowmetadata.error = traceback.format_exc()
                self.database.commit()

                self.get_engine().stop_engine()
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

    def mark_paused(self):
        try:
            self.database.workflowmetadata.please_pause = True
            Logger.info(
                "Marked workflow as paused, this may take some time properly pause"
            )
            self.database.workflowmetadata.commit()
            return True
        except Exception as e:
            Logger.critical("Couldn't mark paused: " + str(e))
            return False

    def get_engine(self):
        if not self._engine:
            self._engine = self.environment.engine
        return self._engine

    def start_engine_if_required(self):
        # engine should be loaded from the DB
        engine = self.get_engine()
        self.environment.engine = engine

        is_allegedly_started = engine.test_connection()

        if is_allegedly_started:
            return

        if not isinstance(engine, Cromwell):
            engine.start_engine()
            return

        additional_cromwell_params = []
        if not engine.config:
            Logger.info("Skipping start database as Janis is not managing the config")
        else:
            dbconfig: JanisDatabaseConfigurationHelper = self.database.workflowmetadata.dbconfig
            dbtype = dbconfig.which_db_to_use()
            if dbtype == dbconfig.DatabaseTypeToUse.existing:
                engine.config.database = dbconfig.get_config_for_existing_config()
            elif dbtype == dbconfig.DatabaseTypeToUse.filebased:
                engine.config.database = dbconfig.get_config_for_filebased_db(
                    path=self.get_path_for_component(self.WorkflowManagerPath.database)
                    + "/cromwelldb"
                )
            elif dbtype == dbconfig.DatabaseTypeToUse.managed:
                cromwelldb_config = self.start_mysql_and_prepare_cromwell_config()
                additional_cromwell_params.append(
                    "-Ddatabase.db.url=" + cromwelldb_config.db.url
                )
                engine.config.database = cromwelldb_config
            else:
                Logger.warn(
                    "Skipping database config as '--no-database' option was provided."
                )

        engine.start_engine(additional_cromwell_options=additional_cromwell_params)
        # Write the new engine details back into the database (for like PID, host and is_started)
        self.database.workflowmetadata.engine = engine

    def start_mysql_and_prepare_cromwell_config(self):
        scriptsdir = self.get_path_for_component(self.WorkflowManagerPath.mysql)

        containerdir = self.get_path_for_component(self.WorkflowManagerPath.database)
        conf = JanisConfiguration.manager()
        if (
            conf
            and conf.template
            and isinstance(conf.template.template, SingularityEnvironmentTemplate)
        ):
            containerdir = conf.template.template.singularity_container_dir

        self.dbcontainer = MySql(
            wid=self.wid,
            container=JanisConfiguration.manager().container,
            datadirectory=self.get_path_for_component(
                self.WorkflowManagerPath.database
            ),
            confdir=scriptsdir,
            forwardedport=find_free_port(),
            containerdir=containerdir,
        )
        self.dbcontainer.start()

        port = self.dbcontainer.forwardedport
        return CromwellConfiguration.Database.mysql(
            username="root", url=f"127.0.0.1:{port}"
        )

    def prepare_and_output_workflow_to_evaluate_if_required(
        self,
        tool: Tool,
        translator: TranslatorBase,
        validation: Optional[ValidationRequirements],
        batchrun: Optional[BatchRunRequirements],
        hints: Dict[str, str],
        additional_inputs: dict,
        max_cores=None,
        max_memory=None,
        allow_empty_container=False,
        container_override: dict = None,
        check_files=True,
    ) -> Tool:
        if self.database.progressDB.has(ProgressKeys.saveWorkflow):
            return Logger.info(f"Saved workflow from task '{self.wid}', skipping.")

        Logger.debug(f"Saving workflow with id '{tool.id()}' to {translator.name}")

        outdir_workflow = self.get_path_for_component(self.WorkflowManagerPath.workflow)
        translator.translate(
            tool,
            to_console=False,
            to_disk=True,
            hints=hints,
            # This is just the base tool, we're going to potentially transform the inputs
            # and we only really care about the inputs for the workflow we're going to run.
            # We'll store the original workflow to run for provenance, but not to easily rerun
            write_inputs_file=False,
            export_path=os.path.join(outdir_workflow, "original"),
            allow_empty_container=allow_empty_container,
            container_override=container_override,
        )

        Logger.info(f"Saved workflow with id '{tool.id()}' to '{outdir_workflow}'")

        modifiers = []
        if validation:
            modifiers.append(ValidatorPipelineModifier(validation))

        if batchrun:
            modifiers.append(BatchPipelineModifier(batchrun))

        modifiers.append(InputFileQualifierModifier)
        # THIS ONE SHOULD BE LAST

        modifiers.append(InputChecker(check_file_existence=check_files))

        tool_to_evaluate, additional_inputs = PipelineModifierBase.apply_many(
            modifiers, tool, additional_inputs, hints=hints
        )

        translator.translate(
            tool_to_evaluate,
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
            allow_empty_container=allow_empty_container,
            container_override=container_override,
        )

        self.evaluate_output_params(
            wf=tool_to_evaluate, additional_inputs=additional_inputs
        )

        self.database.progressDB.set(ProgressKeys.saveWorkflow)
        return tool_to_evaluate

    def evaluate_output_params(self, wf: Tool, additional_inputs: dict):

        mapped_inps = CwlTranslator().build_inputs_file(
            wf, recursive=False, additional_inputs=additional_inputs
        )
        output_names: Dict[str, any] = {}
        output_folders: Dict[str, any] = {}

        if isinstance(wf, Workflow):
            for o in wf.output_nodes.values():
                output_names[o.id()] = self.evaluate_output_selector(
                    o.output_name, mapped_inps
                )
                output_folders[o.id()] = self.evaluate_output_selector(
                    o.output_folder, mapped_inps
                )

        outputs: List[WorkflowOutputModel] = []

        for o in wf.tool_outputs():
            # We'll
            ext = None
            innertype = o.outtype
            iscopyable = isinstance(o.outtype, (File, Directory)) or (
                isinstance(o.outtype, Array)
                and isinstance(o.outtype.fundamental_type(), (File, Directory))
            )
            while isinstance(innertype, Array):
                innertype = innertype.subtype()
            if isinstance(o.outtype, File):
                ext = o.outtype.extension
            outputs.append(
                WorkflowOutputModel(
                    tag=o.id(),
                    iscopyable=iscopyable,
                    original_path=None,
                    new_path=None,
                    timestamp=None,
                    output_name=output_names.get(o.id()),
                    output_folder=output_folders.get(o.id()),
                    secondaries=o.outtype.secondary_files(),
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
            f"Janis assistant cannot evaluate selecting the output from a {type(selector).__name__} type"
        )

    def submit_workflow_if_required(self):
        if self.database.progressDB.has(ProgressKeys.submitWorkflow):
            return Logger.log(f"Workflow '{self.wid}' has submitted, skipping")

        fn_wf = self.database.workflowmetadata.submission_workflow
        fn_inp = self.database.workflowmetadata.submission_inputs
        fn_deps = self.database.workflowmetadata.submission_resources

        engine = self.get_engine()

        Logger.debug(f"Submitting task '{self.wid}' to '{engine.id()}'")
        self._engine_wid = engine.start_from_paths(self.wid, fn_wf, fn_inp, fn_deps)
        self.database.workflowmetadata.engine_wid = self._engine_wid

        Logger.info(
            f"Submitted workflow ({self.wid}), got engine id = '{self.get_engine_wid()}'"
        )
        self.database.progressDB.set(ProgressKeys.submitWorkflow)

    def save_metadata_if_required(self):
        if self.database.progressDB.has(ProgressKeys.savedMetadata):
            return Logger.debug(f"Workflow '{self.wid}' has saved metadata, skipping")

        engine = self.get_engine()

        metadir = self.get_path_for_component(self.WorkflowManagerPath.metadata)
        if isinstance(engine, Cromwell):
            import json

            meta = engine.raw_metadata(self.get_engine_wid()).meta
            with open(os.path.join(metadir, "metadata.json"), "w+") as fp:
                json.dump(meta, fp)

        elif isinstance(engine, CWLTool):
            import json

            meta = engine.metadata(self.wid)
            self.set_status(meta.status)
            with open(os.path.join(metadir, "metadata.json"), "w+") as fp:
                json.dump(meta.outputs, fp)

        else:
            raise Exception(
                f"Don't know how to save metadata for engine '{engine.id()}'"
            )

        self.database.progressDB.set(ProgressKeys.savedMetadata)

    def copy_outputs_if_required(self):
        if self.database.progressDB.has(ProgressKeys.copiedOutputs):
            return Logger.debug(f"Workflow '{self.wid}' has copied outputs, skipping")

        if self.database.workflowmetadata.status != TaskStatus.COMPLETED:
            return Logger.warn(
                f"Skipping copying outputs as workflow "
                f"status was not completed ({self.database.workflowmetadata.status})"
            )

        wf_outputs = self.database.outputsDB.get_all()
        engine_outputs = self.get_engine().outputs_task(self.get_engine_wid())
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
                iscopyable=out.iscopyable,
            )

            if isinstance(originalfile, list):
                originalfile = recursively_join(originalfile, "|")

            if isinstance(newfilepath, list):
                newfilepath = recursively_join(newfilepath, "|")

            self.database.outputsDB.update_paths(
                tag=out.tag, original_path=originalfile, new_path=newfilepath
            )

        self.database.progressDB.set(ProgressKeys.copiedOutputs)
        Logger.info(f"View the task outputs: file://{self.get_task_path()}")

    def copy_output(
        self,
        fs: FileScheme,
        outputid,
        prefix,
        tag,
        secondaries,
        extension,
        iscopyable,
        engine_output: Union[WorkflowOutputModel, Any, List[Any]],
        shard=None,
    ):

        # the output_folder is an array of an array, for each

        if isinstance(engine_output, list):
            outs = []
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

                new_shard = [*prev_shards, i]

                # choose tag
                new_prefix = prefix
                if isinstance(new_prefix, list) and len(new_prefix) > 1:
                    new_prefix = new_prefix[i]
                    new_shard = new_shard[min(len(new_shard), 1) :]

                new_tag = tag
                if tag_index_to_explode is not None:
                    new_tag = explode_at_index(tag, tag_index_to_explode, i)
                    new_shard = new_shard[min(len(new_shard), 1) :]

                outs.append(
                    self.copy_output(
                        fs,
                        outputid=outputid,
                        tag=new_tag,
                        prefix=new_prefix,
                        engine_output=eout,
                        shard=new_shard,
                        secondaries=secondaries,
                        extension=extension,
                        iscopyable=iscopyable,
                    )
                )

            return [o[0] for o in outs], [o[1] for o in outs]

        final_tags = tag

        outfn = outputid

        if final_tags and any(isinstance(t, list) for t in final_tags):
            Logger.critical(
                f"One of the final output tags {str(final_tags)} was still an array, outputs will be written directly into the output directory"
            )
            final_tags = None

        if prefix:
            if isinstance(prefix, list):
                if len(prefix) > 1:
                    Logger.critical(
                        f"Expected only one output_name for this copy, but found ({', '.join(prefix)}) [{len(prefix)}], using the first outputname"
                    )
                else:
                    outfn = prefix[0]
            else:
                outfn = prefix

        if final_tags is None:
            final_tags = []

        outdir = os.path.join(self.path, "/".join(final_tags))

        fs.mkdirs(outdir)

        if shard is not None:
            for s in shard:
                outfn += f"_shard-{s}"

        # copy output

        original_filepath = None
        newoutputfilepath = os.path.join(outdir, outfn)

        if isinstance(engine_output, WorkflowOutputModel):
            original_filepath = engine_output.originalpath
            if original_filepath and iscopyable:
                ext = extension or get_extension(engine_output.originalpath)
                if ext:
                    dot = "" if ext[0] == "." else "."
                    outfn += dot + ext
                    newoutputfilepath += dot + ext
                fs.cp_from(engine_output.originalpath, newoutputfilepath, force=True)
            elif engine_output.value:
                if isinstance(fs, LocalFileScheme):
                    # Write engine_output to outpath
                    with open(newoutputfilepath, "w+") as outfile:
                        outfile.write(str(engine_output.value))
        else:
            original_filepath = engine_output
            if isinstance(fs, LocalFileScheme):
                # Write engine_output to outpath
                with open(newoutputfilepath, "w+") as outfile:
                    outfile.write(str(engine_output))

        for sec in secondaries or []:
            frompath = apply_secondary_file_format_to_filename(original_filepath, sec)
            tofn = apply_secondary_file_format_to_filename(outfn, sec)
            topath = os.path.join(outdir, tofn)
            fs.cp_from(frompath, topath, force=True)

        return [original_filepath, newoutputfilepath]

    def remove_exec_dir(self):
        status = self.database.workflowmetadata.status

        keep_intermediate = self.database.workflowmetadata.keepexecutiondir
        if (
            not keep_intermediate
            and status is not None
            and status == TaskStatus.COMPLETED
        ):
            execdir = self.get_path_for_component(self.WorkflowManagerPath.execution)
            if execdir and execdir != "None":
                Logger.info("Cleaning up execution directory")
                self.environment.filescheme.rm_dir(execdir)
                self.database.progressDB.set(ProgressKeys.cleanedUp)

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
        # if not self.database.progressDB.has(ProgressKeys.savedLogs):
        #     return Logger.debug(f"Workflow '{self.wid}' has copied logs, skipping")

        jobs = self.database.jobsDB.get_all()

        # iterate through all jobs
        for j in jobs:
            od = self.get_path_for_component(self.WorkflowManagerPath.logs)
            base = j.jid
            if j.shard:
                base += "shard-" + str(j.shard)
            on_base = os.path.join(od, base)

            if j.stdout and os.path.exists(j.stdout):
                self.environment.filescheme.cp_from(
                    j.stdout, on_base + "_stdout", force=True
                )

            if j.stderr and os.path.exists(j.stderr):
                self.environment.filescheme.cp_from(
                    j.stderr, on_base + "_stderr", force=True
                )

    def set_status(self, status: TaskStatus, force_notification=False):
        prev = self.database.workflowmetadata.status

        if prev == status and not force_notification:
            return

        Logger.info("Status changed to: " + str(status))
        self.database.workflowmetadata.status = status
        self.database.commit()
        # send an email here

        NotificationManager.notify_status_change(status, self.database.get_metadata())

    def save_metadata(self, meta: WorkflowModel) -> Optional[bool]:
        if not meta:
            return None

        self.database.save_metadata(meta)

        self.set_status(meta.status)

        return meta.status in TaskStatus.final_states()

    @staticmethod
    def mark_aborted(outputdir, wid: Optional[str]) -> bool:
        try:
            if not wid:
                db = WorkflowManager.from_path_get_latest(
                    outputdir, readonly=False
                ).database.workflowmetadata
            else:
                db = WorkflowDbManager.get_workflow_metadatadb(
                    outputdir, wid, readonly=False
                )
            db.please_abort = True
            db.kvdb.commit()
            db.close()
            Logger.info("Marked workflow as aborted, this may take some time full exit")
            return True
        except Exception as e:
            Logger.critical("Couldn't mark aborted: " + str(e))
            return False

    def abort(self) -> bool:
        self.set_status(TaskStatus.ABORTED, force_notification=True)
        status = False

        engine = self.get_engine()
        try:
            status = bool(engine.terminate_task(self.get_engine_wid()))
        except Exception as e:
            Logger.critical("Couldn't abort task from engine: " + str(e))
        try:
            engine.stop_engine()
            if self.dbcontainer:
                self.dbcontainer.stop()
        except Exception as e:
            Logger.critical("Couldn't stop engine: " + str(e))

        return status

    def stop_computation(self):
        try:
            # reset pause flag
            self.database.commit()

            self.get_engine().stop_engine()
            if self.dbcontainer:
                self.dbcontainer.stop()

            self.database.close()

        except Exception as e:
            Logger.critical(
                "An error occurred while trying to pause Janis state: "
                + str(e)
                + "\n\nSee the logfile for more information: "
                + Logger.WRITE_LOCATION
            )
        Logger.close_file()

    @staticmethod
    def _create_dir_if_needed(path):
        if not os.path.exists(path):
            os.makedirs(path)
