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
import sys
import time
from datetime import datetime
from io import StringIO
from shutil import rmtree
from enum import Enum
from subprocess import call
from typing import Optional, List, Dict, Union, Any, Tuple

from janis_assistant.modifiers.cwlinputobjectunwrappermodifier import (
    CwlInputObjectUnwrapperModifier,
)
from janis_assistant.utils.callprogram import collect_output_from_command
from janis_core import (
    Logger,
    File,
    Array,
    LogLevel,
    Directory,
    Tool,
    WorkflowBase,
)
from janis_core.__meta__ import GITHUB_URL
from janis_core.operators.operator import Operator
from janis_core.translations import get_translator, CwlTranslator
from janis_core.translations.translationbase import TranslatorBase
from janis_core.translations.wdl import apply_secondary_file_format_to_filename

from janis_assistant.data.enums import TaskStatus, ProgressKeys
from janis_assistant.data.models.joblabel import JobLabelModel
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.data.models.run import SubmissionModel, RunModel
from janis_assistant.data.providers.workflowmetadataprovider import SubmissionDbMetadata
from janis_assistant.engines import (
    get_ideal_specification_for_engine,
    Cromwell,
    CWLTool,
    CromwellConfiguration,
    Engine,
    EngineType,
)
from janis_assistant.management.configuration import (
    JanisConfiguration,
    JanisDatabaseConfigurationHelper,
    DatabaseTypeToUse,
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
    convert_value_or_list_to_string,
)
from janis_assistant.utils.batchrun import BatchRunRequirements
from janis_assistant.utils.dateutils import DateUtil
from janis_assistant.utils.getuser import lookup_username
from janis_assistant.validation import ValidationRequirements


class WorkflowManager:

    MAX_ENGINE_ATTEMPTS = 5
    HEALTH_CHECK_INTERVAL_SECONDS = 15 * 60
    HEALTH_CHECK_STUCK_CROMWELL_MIN_RUNNING_TIME_SECONDS = 10 * 60

    class WorkflowManagerPath(Enum):
        execution = "execution"
        workflow = "workflow"
        metadata = "metadata"
        logs = "logs"
        configuration = "configuration"
        database = "database"
        mysql = "configuration/mysql"
        semaphore = "semaphore"

    def __init__(
        self,
        execution_dir: str,
        submission_id: str,
        engine: Engine = None,
        readonly=False,
    ):
        # do stuff here
        self.submission_id = submission_id

        self._failed_engine_attempts = None

        # hydrate from here if required
        self._engine_id = None
        self.execution_dir = fully_qualify_filename(execution_dir)
        self.create_dir_structure(self.execution_dir)

        self.database = WorkflowDbManager(
            submission_id, self.get_task_path_safe(), readonly=readonly
        )
        self.dbcontainer: MySql = None
        self.main_queue = queue.Queue()

        self._prev_status = None
        self.engine: Optional[Engine] = engine

        # mfranklin: 2020-08-04
        # I removed the ability for you to specify non-local
        #  fileschemes that janis could copy from / to.
        self.filescheme = LocalFileScheme()

        self.health_check_meta = None

        if not self._engine_id:
            self._engine_id = self.get_engine_id()

    @staticmethod
    def has(
        outdir: str,
        submission_id: str,
        status: Optional[TaskStatus],
        name: Optional[str] = None,
    ):
        metadb: SubmissionDbMetadata = WorkflowDbManager.get_workflow_metadatadb(
            outdir, submission_id
        ).metadata

        has = True
        if has and status:
            has = metadb.status == status
        if has and name:
            has = metadb.name.lower().startswith(name.lower())

        return metadb if has else False

    def watch(self, **kwargs):
        self.show_status_screen(**kwargs)

    @staticmethod
    def from_janis(
        submission_id: str,
        tool: Tool,
        prepared_submission: PreparedJob,
        engine: Engine,
        wait=False,
    ):

        # output directory has been created

        tm = WorkflowManager(
            submission_id=submission_id,
            execution_dir=prepared_submission.execution_dir,
            engine=engine,
        )

        # let's write out the prepared_submission for
        tm.write_prepared_submission_file(
            prepared_job=prepared_submission, output_dir=tm.execution_dir,
        )

        tm.database.submissions.insert_or_update_many(
            [
                SubmissionModel(
                    id_=submission_id,
                    output_dir=prepared_submission.output_dir,
                    execution_dir=prepared_submission.execution_dir,
                    author=lookup_username(),
                    labels=[],
                    tags=[],
                    timestamp=DateUtil.now(),
                    engine_type=engine.id(),
                    engine_url=None,
                )
            ]
        )

        tm.database.submission_metadata.set_metadata(
            SubmissionDbMetadata(
                submission_id=submission_id,
                run_id=RunModel.DEFAULT_ID,
                engine=engine,
                name=tool.id(),
                start=DateUtil.now(),
                keep_execution_dir=prepared_submission.keep_intermediate_files,
                prepared_job=prepared_submission,
                db_configuration=prepared_submission.cromwell.get_database_config_helper()
                if prepared_submission.cromwell
                else None,
                # This is the only time we're allowed to skip the tm.set_status
                # This is a temporary stop gap until "notification on status" is implemented.
                # tm.set_status(TaskStatus.PROCESSING)
                status=TaskStatus.PROCESSING,
            )
        )

        spec = get_ideal_specification_for_engine(engine)
        spec_translator = get_translator(spec)
        tool_evaluate = tm.prepare_and_output_workflow_to_evaluate_if_required(
            run_id=RunModel.DEFAULT_ID,
            tool=tool,
            translator=spec_translator,
            validation=prepared_submission.validation,
            batchrun=prepared_submission.batchrun,
            hints=prepared_submission.hints,
            additional_inputs=prepared_submission.inputs,
            max_cores=prepared_submission.environment.max_cores,
            max_memory=prepared_submission.environment.max_memory,
            max_duration=prepared_submission.environment.max_duration,
            allow_empty_container=prepared_submission.allow_empty_container,
            container_override=prepared_submission.container_override,
            check_files=not prepared_submission.skip_file_check,
            skip_digest_lookup=prepared_submission.skip_digest_lookup,
            skip_digest_cache=prepared_submission.skip_digest_cache,
            cache_location=prepared_submission.digest_cache_location,
        )

        outdir_workflow = tm.get_path_for_component(
            WorkflowManager.WorkflowManagerPath.workflow
        )

        tm.database.submission_metadata.metadata.submission_workflow = os.path.join(
            outdir_workflow, spec_translator.filename(tool_evaluate)
        )
        tm.database.submission_metadata.metadata.submission_inputs = os.path.join(
            outdir_workflow, spec_translator.inputs_filename(tool_evaluate)
        )
        tm.database.submission_metadata.metadata.submission_resources = os.path.join(
            outdir_workflow, spec_translator.dependencies_filename(tool_evaluate)
        )

        tm.database.submission_metadata.save_changes()
        tm.database.commit()

        is_dry_run = False
        if not is_dry_run:
            if (
                not prepared_submission.run_in_background
                and prepared_submission.template
                and prepared_submission.template.template
                and prepared_submission.template.template.can_run_in_foreground is False
            ):
                raise Exception(
                    f"Your template '{prepared_submission.template.template.__class__.__name__}' is not allowed to run "
                    f"in the foreground, try adding the '--background' argument"
                )
            tm.start_or_submit(
                run_in_background=prepared_submission.run_in_background,
                watch=prepared_submission.should_watch_if_background,
            )
        else:
            tm.set_status(TaskStatus.DRY_RUN)

        tm.database.commit()

        if wait:
            Logger.info("WAITING until task finishes before returning")
            while not tm.database.get_uncached_status().is_in_final_state():
                time.sleep(2)

        return tm

    def start_or_submit(self, run_in_background, watch=False):
        # check container environment is loaded
        metadb = self.database.submission_metadata.metadata

        jc = metadb.prepared_job
        PreparedJob._instance = jc
        metadb.containertype = jc._container.__name__
        metadb.containerversion = jc._container.test_available_by_getting_version()

        self.database.submission_metadata.save_changes()

        # resubmit the engine
        if not run_in_background:
            return self.resume()

        # this happens for all workflows no matter what type

        wid = metadb._submission_id

        loglevel = LogLevel.get_str(Logger.CONSOLE_LEVEL)
        command = [
            "janis",
            "--logLevel",
            loglevel,
            "resume",
            "--foreground",
            self.execution_dir,
        ]
        scriptdir = self.get_path_for_component(self.WorkflowManagerPath.configuration)
        logdir = self.get_path_for_component(self.WorkflowManagerPath.logs)

        # prepare recently running information
        logpath = os.path.join(logdir, f"janis-monitor.{self.submission_id}.debug.log")
        Logger.debug("Logging DEBUG to " + logpath)
        try:
            additional_information = jc.template.template.submit_detatched_resume(
                wid=wid, command=command, scriptdir=scriptdir, logsdir=logdir, config=jc
            )
            if additional_information:
                additional_information = f"Received response: {additional_information}"
            self.set_status(
                TaskStatus.QUEUED, additional_information=additional_information
            )
            Logger.info("Submitted detatched engine")
        except Exception as e:
            m = f"There was an error submitting the janis workflow, {e}"
            Logger.critical(m)
            self.set_status(TaskStatus.FAILED, error=m)

        if watch:
            Logger.log("Watching submitted workflow")
            self.show_status_screen()

    @staticmethod
    def from_path_with_submission_id(path: str, submission_id: str, readonly=False):
        """
        :param submission_id: Workflow ID
        :param path: Path to workflow
        :return: TaskManager after resuming (might include a wait)
        """
        # mfranklin 2020-07-09:
        # we need to check in two places:
        #   1. The directory they specified
        #   2. In a subdirectory called "janis"

        if not os.path.exists(path):
            raise Exception("Execution path '{path}' did not exist")

        if not os.path.exists(
            WorkflowManager.get_path_for_component_and_dir(
                path, WorkflowManager.WorkflowManagerPath.database
            )
        ):
            raise Exception(
                f"Couldn't find a 'task.db' in the execution directory '{path}', did you specify the output directory instead?"
            )

        db = WorkflowDbManager.get_workflow_metadatadb(
            path, submission_id, readonly=readonly
        ).metadata

        if not submission_id:
            submission_id = db.submission_id  # .get_meta_info(InfoKeys.taskId)

        if not submission_id:
            raise Exception(f"Couldn't find workflow with id '{submission_id}'")

        # db.close()

        tm = WorkflowManager(
            execution_dir=path,
            submission_id=submission_id,
            engine=db.engine,
            readonly=readonly,
        )
        return tm

    @staticmethod
    def from_path_get_latest_submission_id(path) -> Tuple[str, str]:
        """
        :return: (execution_path, sid)
        """
        paths = [path, os.path.join(path, "janis")]
        for pth in paths:

            if not os.path.exists(WorkflowDbManager.get_sql_path_base(pth)):
                continue

            return pth, WorkflowDbManager.get_latest_submission_id(path=pth)

        raise Exception(
            f"Couldn't find the task database (task.db) in the path '{paths[0]}'"
            f" (this might be different to the output directory)"
        )

    @staticmethod
    def from_path_get_latest_manager(path, readonly=False):

        (execpath, submission_id) = WorkflowManager.from_path_get_latest_submission_id(
            path
        )
        return WorkflowManager.from_path_with_submission_id(
            execpath, submission_id, readonly=readonly
        )

    def show_status_screen(self, **kwargs):
        """
        This function just polls the database for metadata every so often,
        and simply displays it. It will keep doing that until the task
        moves into a TERMINAL status.

        It's presumed that there's a janis-monitor that's watching the engine.

        The kwargs argument is for passing through formatting/refresh options
        through from the command-line.
        """

        if self.database.progressDB.has(ProgressKeys.workflowMovedToFinalState):

            meta = self.database.get_metadata()
            for idx in range(1, 6):
                if meta is not None:
                    break
                Logger.info(
                    f"Couldn't get metadata from DB, sleeping for a second then will try again {5 - idx} times"
                )
                time.sleep(1)
                meta = self.database.get_metadata()

            formatted = meta.format(**kwargs)
            print(formatted)
            return Logger.debug(
                f"Workflow '{self.submission_id}' has already finished, skipping"
            )

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
            self.poll_stored_metadata_with_clear(**kwargs)

    def get_meta_call(self) -> Tuple[Optional[SubmissionModel], bool]:
        meta = self.database.get_metadata()
        if meta is None:
            return None, False

        is_finished = (
            meta is not None
            and meta.runs
            and all(m.status in TaskStatus.final_states() for m in meta.runs)
            and meta.status.is_in_final_state()
        )

        return (
            meta,
            is_finished,
        )

    def poll_stored_metadata_with_clear(self, seconds=3, **kwargs):
        try:
            is_finished = False

            # We won't clear the screen if we haven't printed (first loop) and it's finished
            has_printed = False
            last_updated = None
            metadata_skips = 0
            while not is_finished:
                meta, is_finished = self.get_meta_call()
                if meta:

                    has_updated = (
                        last_updated is not None
                        and bool(meta.runs)
                        and meta.runs[0].last_updated is not None
                        and meta.runs[0].last_updated > last_updated
                    )
                    ignore_has_updated = metadata_skips > 20
                    is_running = not is_finished
                    if (
                        not has_printed
                        or has_updated
                        or ignore_has_updated
                        or is_finished
                    ):
                        metadata_skips = 0
                        if meta.runs:
                            last_updated = meta.runs[0].last_updated

                        # only clear the screen if we haven't updated
                        if has_printed or is_running:

                            try:
                                call("clear")
                            except Exception as e:
                                Logger.log(
                                    f"We got a subprocess error when clearing the screen: {repr(e)}"
                                )
                        print(meta.format(**kwargs))
                        has_printed = True
                    else:
                        Logger.log(
                            f"No job status updates were find, we'll wait another {seconds} seconds"
                        )
                        metadata_skips += 1

                if seconds < 0:
                    is_finished = True

                if not is_finished:
                    time.sleep(seconds)
        except KeyboardInterrupt:
            pass
        Logger.log("Finish watch")

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
        # doesn't necessarily have to be successful to run this block

        status: TaskStatus = self.database.submission_metadata.metadata.status
        Logger.info(f"Task has finished with status: {status}")
        try:
            self.save_metadata_if_required()
        except Exception as e:
            if status == TaskStatus.EXECUTION_ENDED_SUCCESSFULLY:
                Logger.critical(f"Couldn't persist metadata on exit as {repr(e)}")
            else:
                Logger.debug(
                    f"Couldn't persist workflow metadata on exit (this isn't surprising as the workflow was {status})"
                )
        self.copy_logs_if_required()
        self.copy_outputs_if_required()

        self.stop_engine_and_db()

        if status == TaskStatus.EXECUTION_ENDED_SUCCESSFULLY:
            self.cleanup_execution()
            status = TaskStatus.COMPLETED
            self.set_status(TaskStatus.COMPLETED)

        self.run_post_run_script()

        rc = status.get_exit_code()
        if rc != 0:
            # Fail the Janis job with the return code, probably 3
            Logger.critical(
                f"Exiting with rc={rc} janis task '{self.submission_id}' as the workflow was {status}"
            )
            sys.exit(rc)
        else:
            Logger.info(f"Finished managing task '{self.submission_id}'.")

    def run_post_run_script(self):
        pj = PreparedJob.instance()
        if pj and pj.post_run_script:
            try:
                command = " ".join(
                    [
                        pj.post_run_script,
                        self.submission_id,
                        self.execution_dir,
                        self.database.submission_metadata.metadata.status.to_string(),
                    ]
                )
                collect_output_from_command(
                    command=command,
                    stdout=Logger.guess_log,
                    stderr=Logger.guess_log,
                    shell=True,
                )
            except Exception as e:
                Logger.critical(f"Failed to execute post-run-script with: {repr(e)}")

    def resume(self):
        """
        Resume takes an initialised DB, looks for the engine (if it's around),
        or starts it and monitors the status, updating the DB every so often
        :return:
        """

        # get a logfile and start doing stuff
        rc = 0

        meta = self.database.get_metadata()
        status = meta.status
        if status in TaskStatus.final_states():
            return Logger.info(
                f"This task has already finished and cannot be resumed, view the task outputs at file://{meta.output_dir}"
            )

        try:
            # remove semaphores
            self.remove_semaphores()

            verbosepath = self.get_log_path_for_level(level=LogLevel.VERBOSE)
            debugpath = self.get_log_path_for_level(level=LogLevel.DEBUG)
            infopath = self.get_log_path_for_level(level=LogLevel.INFO)
            warnpath = self.get_log_path_for_level(level=LogLevel.WARNING)

            Logger.WRITE_LEVELS = {
                LogLevel.VERBOSE: (verbosepath, open(verbosepath, "a")),
                LogLevel.DEBUG: (debugpath, open(debugpath, "a")),
                LogLevel.INFO: (infopath, open(infopath, "a")),
                LogLevel.WARNING: (warnpath, open(warnpath, "a")),
            }
            Logger.log(
                "Set write levels to: "
                + self.get_path_for_component(self.WorkflowManagerPath.logs)
            )
            Logger.info("Logging debug information to: " + str(debugpath))

            if True:
                from janis_assistant.__meta__ import __version__

                Logger.debug(f"Using janis-assistant: {__version__}")

            # in case anything relies on CD, we'll throw it into janis/execution
            os.chdir(self.get_path_for_component(self.WorkflowManagerPath.execution))

            self.start_engine_if_required()

            if os.path.exists(self.get_abort_semaphore_path()):
                Logger.info("Detected please_abort request, aborting")
                return self.abort()

            if os.path.exists(self.get_pause_semaphore_path()):
                Logger.info("Detected please_pause request, exiting")
                return self.suspend_workflow()

            # check status and see if we can resume
            self.submit_workflow_if_required()

            self.database.commit()

            # Now the workflow is running, get status every so often, save it and just wait

            def callb(meta: RunModel):
                meta.submission_id = self.submission_id
                meta.id_ = RunModel.DEFAULT_ID
                meta.apply_ids_to_children()

                if meta.status == TaskStatus.COMPLETED:
                    meta.status = TaskStatus.EXECUTION_ENDED_SUCCESSFULLY

                self.main_queue.put(lambda: self.save_metadata(meta)),

            self.engine.add_callback(self.get_engine_id(), callback=callb)

            # add extra check for engine on resume
            meta = self.engine.metadata(self.get_engine_id())
            if meta and meta.status in TaskStatus.final_states():
                self.save_metadata(meta)
                return self.process_completed_task()

            last_semaphore_check = None
            last_health_check = DateUtil.now()

            while True:
                try:
                    cb = self.main_queue.get(False)
                    # callback from add_callback() returns True if in TaskStatus.final_states()
                    res = cb()
                    if res is True:
                        break
                except queue.Empty:
                    if (
                        last_semaphore_check is None
                        or (DateUtil.now() - last_semaphore_check).total_seconds() > 2
                    ):
                        Logger.log("Checking semaphores")
                        last_semaphore_check = DateUtil.now()
                        if os.path.exists(self.get_abort_semaphore_path()):
                            self.abort()
                            break
                        if os.path.exists(self.get_pause_semaphore_path()):
                            self.suspend_workflow()
                            break

                    # # TODO: make this interval be a config option
                    # if (
                    #     DateUtil.now() - last_health_check
                    # ).total_seconds() > self.HEALTH_CHECK_INTERVAL_SECONDS:
                    #     self.do_health_check()
                    #     last_health_check = DateUtil.now()

                    time.sleep(0.5)

                    continue
                except Exception as e:
                    Logger.warn(f"Something has gone TERRIBLY wrong: {repr(e)}")
                    raise e

            self.process_completed_task()
            return self

        except Exception as e:
            import traceback

            err = traceback.format_exc()
            Logger.critical(
                f"A fatal error occurred while monitoring workflow = '{self.submission_id}', exiting: {e}:\n {err}"
            )

            try:
                rc = TaskStatus.FAILED.get_exit_code()
                self.set_status(TaskStatus.FAILED)
                self.database.submission_metadata.metadata.error = (
                    traceback.format_exc()
                )
                self.database.commit()

                self.stop_engine_and_db()

                self.database.close()
            except Exception as e:
                Logger.critical(
                    f"An additional fatal error occurred while trying to store Janis state: {str(e)}\n\n"
                    f"See the logfile for more information: {self.get_log_path_for_level(level=LogLevel.DEBUG)}"
                )

        Logger.close_file()

        if rc != 0:
            Logger.critical(
                f"Exiting with rc={rc} janis task '{self.submission_id}' as an issue occurred when running the workflow"
            )
            sys.exit(rc)

    def stop_engine_and_db(self):
        self.engine.stop_engine()

        if self.dbcontainer:
            self.dbcontainer.stop()

    def do_health_check(self):

        # Checks to run:
        #   - Check that all in-progress jobs have files we expect
        #   - Ask the template to validate the metadata as well

        # If all the runnings jobs have warnings, and these warnings are
        # identical between health checks, then we'll abort the workflow

        # loop through to get all in-progress jobs with a
        Logger.debug("Performing health check")

        is_cromwell = isinstance(self.engine, Cromwell)

        previous_meta = self.health_check_meta or {}
        new_health_meta = {}

        if is_cromwell:
            new_health_meta[
                "STUCK_CROMWELL_JOBS"
            ] = self.check_cromwell_stuck_running_jobs()

        # check the two metas, if they're both non-empty and equal, then we fail
        filtered_previous_meta = {
            k: v for k, v in previous_meta.items() if v is not None
        }
        filtered_new_meta = {k: v for k, v in new_health_meta.items() if v is not None}
        if filtered_new_meta and filtered_previous_meta:

            if filtered_new_meta == previous_meta:
                Logger.critical(
                    f"This submission {self.submission_id} failed the health check with metadata '{filtered_new_meta}' for the second time an will now abort."
                )
                self.abort()
                return False
            Logger.warn(
                f"Janis failed the health check for the first time with metadata '{filtered_new_meta}'"
            )

        self.health_check_meta = new_health_meta

        return True

    def check_cromwell_stuck_running_jobs(self):
        total_running_jobs = 0
        # We've seen this scenario where
        jobs_stuck_in_running_state = []

        jobs = self.database.jobsDB.get_all(
            additional_where=("status = ?", [TaskStatus.RUNNING.value])
        )

        if not jobs:
            Logger.debug(
                "No running jobs were returned when checking stuck running jobs during health check"
            )
            return None

        Logger.log(
            f"Checking {len(jobs)} jobs for stuck cromwell jobs during health check"
        )

        good_jobs = []

        for job in jobs:

            # Janis can't really tell what's a placeholder job (like a subworkflow / scatter parent),
            # and what's a real job, so we'll rely on the job setting the workdir for this to work.

            if job.workdir is None or job.status != TaskStatus.RUNNING:
                continue

            total_running_jobs += 1

            # Anecdotally, cromwell moves a job into the running state sometimes _just_ before it's
            job_last_updated = (DateUtil().now() - job.lastupdated).total_seconds()
            if (
                job_last_updated
                < self.HEALTH_CHECK_STUCK_CROMWELL_MIN_RUNNING_TIME_SECONDS
            ):
                continue

            script_location = os.path.join(job.workdir, "execution/script")
            if not os.path.exists(script_location):
                jobs_stuck_in_running_state.append(job.id_)
            else:
                good_jobs.append(job.id_)

        if not jobs_stuck_in_running_state:
            return None

        Logger.warn(
            f"Detected {len(jobs_stuck_in_running_state)}(out of {total_running_jobs} checked) stuck cromwell jobs: {', '.join(jobs_stuck_in_running_state)}"
        )
        NotificationManager.send_email(
            subject=f"[WARN] Workflow {self.submission_id} had {len(jobs_stuck_in_running_state)} stuck running jobs",
            body=f"""

<table>
    <tr>
        <td>Submission ID</td>  <td>{self.submission_id}</td>
    </tr><tr>
        <td>Execution Dir</td>  <td>{self.execution_dir}</td>
    </tr><tr>
        <td>Timestamp</td>      <td>{datetime.now()}</td>
    </tr>
</table>
    
Janis has detected that an error has occurred when preparing the following jobs:

<ul>
    <li>{", ".join(jobs_stuck_in_running_state)}</li>
</ul>

Janis suspects this might be due to a disk error when Cromwell was preparing the job. 
Janis will suspend the job once all other tasks have completed or failed.

You may receive this email multiple times.

<br />

Kind regards,
<br />

- Janis
""",
        )

        if total_running_jobs != len(jobs_stuck_in_running_state):
            # We want to return None (no health problem) until
            # all the non-stuck running jobs have finished
            Logger.debug(
                f"Health check technically passed because Janis believes there are jobs still running "
                f"({jobs_stuck_in_running_state}: {', '.join(good_jobs)}) that aren't "
                f"failures ({len(jobs_stuck_in_running_state)}: {', '.join(jobs_stuck_in_running_state)})"
            )
            return None

        Logger.debug(
            "Janis FAILED the stuck running cromwell jobs during the health check"
        )

        return {
            "stuck_jobs": jobs_stuck_in_running_state,
            "total_jobs": total_running_jobs,
        }

    @staticmethod
    def mark_paused(execution_dir):
        try:
            path = os.path.join(
                WorkflowManager.get_path_for_component_and_dir(
                    execution_dir, WorkflowManager.WorkflowManagerPath.semaphore
                ),
                "pause",
            )
            with open(path, "w+") as f:
                f.write(f"Requesting pause {DateUtil.now()}")
            Logger.info(
                "Marked workflow as paused, this may take some time properly pause"
            )
            return True
        except Exception as e:
            Logger.critical("Couldn't mark paused: " + str(e))
            return False

    def start_engine_if_required(self):
        # engine should be loaded from the DB
        engine = self.engine

        is_allegedly_started = engine.test_connection()

        if is_allegedly_started:
            return

        if not isinstance(engine, Cromwell):
            engine.start_engine()
            return

        if not engine.is_managing_cromwell:
            raise Exception(
                f"Janis isn't managing Cromwell, and couldn't connect on '{engine.host}'"
            )

        additional_cromwell_params = []
        if not engine.config:
            Logger.info("Skipping start database as Janis is not managing the config")
        else:
            db_config: JanisDatabaseConfigurationHelper = (
                self.database.submission_metadata.metadata.db_configuration
            )
            dbtype = db_config.db_type
            if dbtype == DatabaseTypeToUse.existing:
                engine.config.database = db_config.get_config_for_existing_config()
            elif dbtype == DatabaseTypeToUse.filebased:
                engine.config.database = db_config.get_config_for_filebased_db(
                    path=self.get_path_for_component(self.WorkflowManagerPath.database)
                    + "/cromwelldb"
                )
            elif dbtype == DatabaseTypeToUse.managed:
                cromwelldb_config = self.start_mysql_and_prepare_cromwell_config()
                additional_cromwell_params.append(
                    "-Ddatabase.db.url=" + cromwelldb_config.db.url
                )
                engine.config.database = cromwelldb_config
            elif dbtype == DatabaseTypeToUse.from_script:
                engine.config.database = db_config.get_config_from_script(
                    self.execution_dir
                )
            else:
                Logger.warn(
                    "Skipping database configuration as '--no-database' option was provided."
                )

        engine_is_started = engine.start_engine(
            additional_cromwell_options=additional_cromwell_params
        )
        if engine_is_started is False or engine_is_started is None:
            raise Exception(f"The engine '{engine.id()}' was not be started, returning")
        # Write the new engine details back into the database (for like PID, host and is_started)
        self.database.submission_metadata.metadata.engine = engine

    def start_mysql_and_prepare_cromwell_config(self):
        scriptsdir = self.get_path_for_component(self.WorkflowManagerPath.mysql)

        containerdir = self.get_path_for_component(self.WorkflowManagerPath.database)
        conf = PreparedJob.instance()
        if (
            conf
            and conf.template
            and isinstance(conf.template.template, SingularityEnvironmentTemplate)
        ):
            containerdir = conf.template.template.singularity_container_dir

        self.dbcontainer = MySql(
            wid=self.submission_id,
            container=conf._container,
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

    @staticmethod
    def prepare_container_override(
        tool: Tool,
        container_override: Optional[dict],
        cache_location: str,
        skip_digest_cache=False,
    ):
        from janis_assistant.data.container import get_digests_from_containers

        containermap = tool.containers()
        if container_override:
            containermap.update(container_override)

        reverse_lookup = {}
        for versioned_toolid, container in containermap.items():
            if container is None:
                continue
            key = container.lower()
            reverse_lookup[key] = reverse_lookup.get(key, []) + [versioned_toolid]

        containers_to_lookup = list(reverse_lookup.keys())
        digest_map = get_digests_from_containers(
            containers_to_lookup,
            cache_location=cache_location,
            skip_cache=skip_digest_cache,
        )
        Logger.debug(f"Found {len(digest_map)} docker digests.")
        Logger.log("Found the following container-to-tool lookup table:")
        Logger.log(CwlTranslator.stringify_translated_inputs(reverse_lookup))

        retval = container_override or {}
        for rawcontainer, container_digest in digest_map.items():
            for c in reverse_lookup.get(rawcontainer, []):
                retval[c] = container_digest

        Logger.debug(
            "Found container replacements: \n"
            + CwlTranslator.stringify_translated_inputs(digest_map)
        )

        return retval

    @staticmethod
    def write_prepared_submission_file(
        prepared_job: PreparedJob, output_dir: str, force_write=False,
    ):
        import ruamel.yaml

        yaml = ruamel.yaml.YAML()

        d = prepared_job.to_dict()
        io = StringIO()
        yaml.dump(d, io)
        s = io.getvalue()

        out_job_path = os.path.join(output_dir, "job.yaml")
        out_run_path = os.path.join(output_dir, "run.sh")

        if os.path.exists(out_job_path) and not force_write:
            Logger.warn(
                f"There was already a job file at '{out_job_path}', skipping write"
            )
        else:
            with open(out_job_path, "w+") as f:
                f.write(s)

        if prepared_job._workflow_reference and isinstance(
            prepared_job._workflow_reference, str
        ):
            if os.path.exists(out_run_path) and not force_write:
                Logger.warn(
                    f"There was already a 'run.sh' script at '{out_run_path}', skipping write"
                )
            else:
                with open(out_run_path, "w+") as f:
                    f.write(
                        f"""\
# This script was automatically generated by Janis on {str(datetime.now())}.

janis run \\
    -j {out_job_path} \\
    {prepared_job._workflow_reference}
"""
                    )
        else:
            Logger.info(
                "The workflow name / path wasn't provided, skipping writing a 'run.sh' file"
            )

        return io.getvalue()

    def prepare_and_output_workflow_to_evaluate_if_required(
        self,
        run_id: str,
        tool: Tool,
        translator: TranslatorBase,
        validation: Optional[ValidationRequirements],
        batchrun: Optional[BatchRunRequirements],
        hints: Dict[str, str],
        additional_inputs: dict,
        cache_location: Optional[str],
        max_cores=None,
        max_memory=None,
        max_duration=None,
        allow_empty_container=False,
        container_override: dict = None,
        check_files=True,
        skip_digest_lookup=False,
        skip_digest_cache=False,
    ) -> Tool:
        if self.database.progressDB.has(ProgressKeys.saveWorkflow):
            return Logger.info(
                f"Saved workflow from task '{self.submission_id}', skipping."
            )

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

        modifiers = [CwlInputObjectUnwrapperModifier()]
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

        container_overrides = container_override
        if not skip_digest_lookup:
            container_overrides = self.prepare_container_override(
                tool,
                container_override,
                cache_location=cache_location,
                skip_digest_cache=skip_digest_cache,
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
            max_duration=max_duration,
            allow_empty_container=allow_empty_container,
            container_override=container_overrides,
        )

        mapped_inps = TranslatorBase.build_inputs_file(
            tool_to_evaluate, recursive=False, additional_inputs=additional_inputs
        )

        self.evaluate_job_labels(tool_to_evaluate, inputs=mapped_inps, run_id=run_id)
        self.database.inputsDB.insert_inputs_from_dict(mapped_inps)

        self.evaluate_and_save_output_params(
            wf=tool_to_evaluate, inputs=mapped_inps, run_id=run_id
        )

        self.database.progressDB.set(ProgressKeys.saveWorkflow)
        return tool_to_evaluate

    def evaluate_job_labels(self, tool: Tool, inputs: dict, run_id):
        if not isinstance(tool, WorkflowBase):
            return
        wf: WorkflowBase = tool
        labels = []
        for stp in wf.step_nodes.values():
            if stp.scatter is not None and stp.scatter.labels is not None:
                resolved_label = self.evaluate_output_selector(
                    stp.scatter.labels, inputs
                )
                labels.append(
                    JobLabelModel(self.submission_id, run_id, stp.id(), resolved_label)
                )

        if len(labels) > 0:
            self.database.joblabelsDB.insert_or_update_many(labels)

    @staticmethod
    def evaluate_output_params(wf: Tool, inputs: dict, submission_id, run_id):

        output_names: Dict[str, any] = {}
        output_folders: Dict[str, any] = {}
        output_extensions: Dict[str, str] = {}

        if isinstance(wf, WorkflowBase):
            for o in wf.output_nodes.values():
                output_names[o.id()] = WorkflowManager.evaluate_output_selector(
                    o.output_name, inputs
                )
                output_folders[o.id()] = WorkflowManager.evaluate_output_selector(
                    o.output_folder, inputs
                )
                output_extensions[o.id()] = o.extension

        outputs: List[WorkflowOutputModel] = []

        for o in wf.tool_outputs():
            # We'll
            ext = (
                o.extension
                if hasattr(o, "extension")
                else output_extensions.get(o.id())
            )
            secs = None
            innertype = o.outtype
            while isinstance(innertype, Array):
                innertype = innertype.fundamental_type().received_type()

            iscopyable = isinstance(innertype, (File, Directory))

            if isinstance(innertype, File):
                if ext is None:
                    ext = innertype.extension
                secs = innertype.secondary_files()

            outputs.append(
                WorkflowOutputModel(
                    id_=o.id(),
                    submission_id=submission_id,
                    run_id=run_id,
                    is_copyable=iscopyable,
                    original_path=None,
                    new_path=None,
                    timestamp=None,
                    output_name=output_names.get(o.id(), True),
                    output_folder=output_folders.get(o.id()),
                    secondaries=secs,
                    extension=ext,
                )
            )
        return outputs

    def evaluate_and_save_output_params(self, wf: Tool, inputs: dict, run_id):
        outputs = self.evaluate_output_params(
            wf=wf, inputs=inputs, submission_id=self.submission_id, run_id=run_id
        )

        return self.database.outputsDB.insert_many(outputs)

    @staticmethod
    def evaluate_output_selector(selector, inputs: dict):
        try:
            if selector is None:
                return None
            if isinstance(selector, bool):
                return selector
            return convert_value_or_list_to_string(
                Operator.evaluate_arg(selector, inputs)
            )
        except Exception as e:
            raise Exception(
                f"Janis assistant couldn't evaluate the output {str(selector)}: {repr(e)}"
            )

    def submit_workflow_if_required(self):
        if self.database.progressDB.has(ProgressKeys.submitWorkflow):
            return Logger.log(
                f"Workflow '{self.submission_id}' has submitted, skipping"
            )

        fn_wf = self.database.submission_metadata.metadata.submission_workflow
        fn_inp = self.database.submission_metadata.metadata.submission_inputs
        fn_deps = self.database.submission_metadata.metadata.submission_resources

        engine = self.engine

        Logger.debug(f"Submitting task '{self.submission_id}' to '{engine.id()}'")
        self._engine_id = engine.start_from_paths(
            self.submission_id, fn_wf, fn_inp, fn_deps
        )
        self.database.submission_metadata.metadata.engine_id = self._engine_id

        Logger.info(
            f"Submitted workflow ({self.submission_id}), got engine id = '{self.get_engine_id()}'"
        )
        self.database.progressDB.set(ProgressKeys.submitWorkflow)

    def save_metadata_if_required(self):
        if self.database.progressDB.has(ProgressKeys.savedMetadata):
            return Logger.debug(
                f"Workflow '{self.submission_id}' has saved metadata, skipping"
            )

        engine = self.engine

        metadir = self.get_path_for_component(self.WorkflowManagerPath.metadata)
        if isinstance(engine, Cromwell):
            import json

            meta = engine.raw_metadata(self.get_engine_id())
            nattempts = 5
            for at in range(nattempts):
                if meta is None:
                    Logger.warn(
                        f"Engine '{engine.id()}' didn't return metadata for Janis to persist, will try {nattempts - at - 1} more times"
                    )
                    meta = engine.raw_metadata(self.get_engine_id())
                else:
                    break
            if meta is None:
                Logger.critical("Cromwell didn't return metadata to persist, skipping")
                return

            with open(os.path.join(metadir, "metadata.json"), "w+") as fp:
                json.dump(meta.meta, fp)

        elif isinstance(engine, CWLTool):
            import json

            meta = engine.metadata(self.submission_id)
            with open(os.path.join(metadir, "metadata.json"), "w+") as fp:
                json.dump(meta.outputs, fp)

        else:
            raise Exception(
                f"Don't know how to save metadata for engine '{engine.id()}'"
            )

        self.database.progressDB.set(ProgressKeys.savedMetadata)

    def copy_outputs_if_required(self):
        if self.database.progressDB.has(ProgressKeys.copiedOutputs):
            return Logger.debug(
                f"Workflow '{self.submission_id}' has copied outputs, skipping"
            )

        if (
            self.database.submission_metadata.metadata.status
            != TaskStatus.EXECUTION_ENDED_SUCCESSFULLY
        ):
            return Logger.warn(
                f"Skipping copying outputs as the workflow {self.database.submission_metadata.metadata.status}"
            )

        wf_outputs: List[WorkflowOutputModel] = self.database.outputsDB.get()
        engine_outputs = self.engine.outputs_task(self.get_engine_id())

        submission = self.database.submissions.get_by_id(
            self.submission_id, allow_operational_errors=False
        )
        if not submission:
            raise Exception(
                "An unrecoverable error occurred when copying outputs, unable to get submission from task.db"
            )

        nattempts = 5
        for at in range(nattempts):
            if engine_outputs is None:
                Logger.warn(
                    f"Engine '{self.engine.id()}' didn't return outputs for Janis to copy, will try {nattempts - at - 1} more times"
                )
                engine_outputs = self.engine.outputs_task(self.get_engine_id())
            else:
                break

        if engine_outputs is None or len(engine_outputs) == 0:
            return Logger.critical(
                f"Engine '{self.engine.id()}' didn't return outputs for Janis to copy, skipping"
            )

        eoutkeys = engine_outputs.keys()

        for out in wf_outputs:
            eout = engine_outputs.get(out.id_)

            if eout is None:
                Logger.warn(
                    f"Couldn't find expected output with tag {out.id_}, found outputs ({', '.join(eoutkeys)}"
                )
                continue
            originalfile, newfilepath = self.copy_output(
                fs=self.filescheme,
                output_dir=submission.output_dir,
                outputid=out.id_,
                output_name_prefix=out.output_name,
                output_folders=out.output_folder,
                secondaries=out.secondaries,
                extension=out.extension,
                engine_output=eout,
                iscopyable=out.is_copyable,
            )

            if isinstance(originalfile, list):
                originalfile = recursively_join(originalfile, "|")

            if isinstance(newfilepath, list):
                newfilepath = recursively_join(newfilepath, "|")

            self.database.outputsDB.update_paths(
                run_id=RunModel.DEFAULT_ID,
                tag=out.id_,
                original_path=originalfile,
                new_path=newfilepath,
                value=None if out.is_copyable else eout.value,
            )

        self.database.progressDB.set(ProgressKeys.copiedOutputs)
        Logger.info(f"View the task outputs: file://{submission.output_dir}")

    def copy_output(
        self,
        fs: FileScheme,
        output_dir: str,
        outputid,
        output_name_prefix,
        output_folders,
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

            tag_index_to_explode = find_element_where_length_is(output_folders, nshards)

            for i in range(nshards):
                eout = engine_output[i]

                new_shard = [*prev_shards, i]

                # choose tag
                new_prefix = output_name_prefix
                if nshards > 1 and isinstance(new_prefix, list) and len(new_prefix) > 1:
                    # this is likely a double array of prefixes, so we'll need to grab the new one out
                    new_prefix = new_prefix[i]
                    new_shard = new_shard[min(len(new_shard), 1) :]

                new_tag = output_folders
                if tag_index_to_explode is not None:
                    new_tag = explode_at_index(output_folders, tag_index_to_explode, i)
                    new_shard = new_shard[min(len(new_shard), 1) :]

                outs.append(
                    self.copy_output(
                        fs,
                        output_dir=output_dir,
                        outputid=outputid,
                        output_folders=new_tag,
                        output_name_prefix=new_prefix,
                        engine_output=eout,
                        shard=new_shard,
                        secondaries=secondaries,
                        extension=extension,
                        iscopyable=iscopyable,
                    )
                )

            return [o[0] for o in outs], [o[1] for o in outs]

        final_tags = output_folders

        if final_tags and any(isinstance(t, list) for t in final_tags):
            Logger.critical(
                f"One of the final output tags {str(final_tags)} was still an array, outputs will be written directly into the output directory"
            )
            final_tags = None

        using_original_filename = (
            output_name_prefix is False or output_name_prefix is None
        )
        has_original_path = (
            iscopyable
            and isinstance(engine_output, WorkflowOutputModel)
            and isinstance(engine_output.value, str)
        )
        if using_original_filename and not has_original_path:
            using_original_filename = False
            output_name_prefix = True
            Logger.warn(
                f"The output '{outputid}' requested original filename, but there wasn't a file to copy. "
                "Janis will use the output_id to construct the output name"
            )

        if using_original_filename:
            outfn = os.path.basename(engine_output.value)
        elif output_name_prefix is True:
            outfn = outputid
        else:
            if isinstance(output_name_prefix, list):
                if len(output_name_prefix) > 1:
                    Logger.critical(
                        f"Expected only one output_name for this copy, but found ({', '.join(output_name_prefix)}) [{len(output_name_prefix)}], using the first outputname"
                    )
                outfn = output_name_prefix[0]
            else:
                outfn = output_name_prefix

        if not using_original_filename:
            if shard is not None:
                # add shards if we're not using the original filename AND
                # there's shards we need to unclash
                for s in shard:
                    outfn += f"_shard-{s}"

            ext = extension
            if ext is None and has_original_path:
                ext = get_extension(engine_output.value)
                if ext is not None:
                    ext = "." + ext
            if ext:
                # mfranklin: require user to correctly specific "." in extension
                outfn += ext

        if not isinstance(outfn, str):
            Logger.warn(
                f"An internal issue occurred when producing an output filename for '{outputid}', was expecting to use "
                f"the original filename but this was not resolved correctly (resolved to: '{outfn}' [type: {type(outfn)}]). "
                f"We'll instead use the output_id. Please raise an issue on GitHub ({GITHUB_URL}) for more information."
            )
            outfn = outputid

        if final_tags is None:
            final_tags = []

        outdir = os.path.join(output_dir, "/".join(final_tags))

        fs.mkdirs(outdir)

        # copy output

        original_filepath = None
        newoutputfilepath = os.path.join(outdir, outfn)

        if isinstance(engine_output, WorkflowOutputModel):
            value = engine_output.value
            if iscopyable:
                if value is None:
                    Logger.critical(
                        f"Couldn't copy the output for '{outputid}', as the engine returned no path"
                    )
                else:
                    fs.cp_from(value, newoutputfilepath, force=True)
                    original_filepath = value
            else:
                if value is None:
                    Logger.warn(f"The output '{outputid}' had no value")
                elif isinstance(fs, LocalFileScheme):
                    # Write engine_output to outpath
                    with open(newoutputfilepath, "w+") as outfile:
                        outfile.write(str(engine_output.value))
                else:
                    Logger.warn(
                        f"Skipping writing the output value for '{outputid}' as Janis doesn't support writing values not on the local filesystem"
                    )
        else:
            original_filepath = engine_output
            if isinstance(fs, LocalFileScheme):
                # Write engine_output to outpath
                with open(newoutputfilepath, "w+") as outfile:
                    outfile.write(str(engine_output))
            else:
                Logger.critical(
                    f"Can't write output '{outputid}' to non-local filesystem '{fs.id()}'"
                )

        for sec in secondaries or []:
            frompath = apply_secondary_file_format_to_filename(original_filepath, sec)
            tofn = apply_secondary_file_format_to_filename(outfn, sec)
            topath = os.path.join(outdir, tofn)
            fs.cp_from(frompath, topath, force=True)

        return [original_filepath, newoutputfilepath]

    def cleanup_execution(self):
        status = self.database.submission_metadata.metadata.status

        keep_intermediate = (
            self.database.submission_metadata.metadata.keep_execution_dir
        )
        if (
            not keep_intermediate
            and status is not None
            and status == TaskStatus.EXECUTION_ENDED_SUCCESSFULLY
        ):
            execdir = self.get_path_for_component(self.WorkflowManagerPath.execution)
            os.chdir(self.execution_dir)
            if execdir and execdir != "None":
                Logger.info("Cleaning up execution directory")
                self.filescheme.rm_dir(execdir)
                self.database.progressDB.set(ProgressKeys.cleanedUp)

            dbconfig: JanisDatabaseConfigurationHelper = (
                self.database.submission_metadata.metadata.db_configuration
            )
            if (
                dbconfig is not None
                and self.engine.engtype == EngineType.cromwell
                and dbconfig.which_db_to_use() == DatabaseTypeToUse.from_script
            ):
                dbconfig.run_delete_database_script(self.execution_dir)

    def log_dbtaskinfo(self):
        import tabulate

        # log all the metadata we have:
        results = self.database.submission_metadata.metadata.get_encoded_rows()
        header = ("Key", "Value")
        res = tabulate.tabulate([header, ("wid", self.submission_id), *results])
        print(res)
        return res

    def get_engine_id(self):
        if not self._engine_id:
            self._engine_id = self.database.submission_metadata.metadata.engine_id
        return self._engine_id

    def get_task_path(self):
        return self.execution_dir

    def get_task_path_safe(self):
        path = self.get_task_path()
        WorkflowManager._create_dir_if_needed(path)
        return path

    def get_path_for_component(self, component: WorkflowManagerPath):
        return self.get_path_for_component_and_dir(self.get_task_path_safe(), component)

    def get_log_path_for_level(self, level: int = LogLevel.DEBUG):
        logsdir = self.get_path_for_component(self.WorkflowManagerPath.logs)
        return os.path.join(
            logsdir,
            f"janis-monitor.{self.submission_id}.{LogLevel.get_str(level).lower()}.log",
        )

    @staticmethod
    def get_path_for_component_and_dir(path, component: WorkflowManagerPath):
        val = os.path.join(path, component.value)
        if not os.path.exists(val):
            os.makedirs(val, exist_ok=True)
        return val

    def get_pause_semaphore_path(self):
        return os.path.join(
            self.get_path_for_component(self.WorkflowManagerPath.semaphore), "pause"
        )

    def get_abort_semaphore_path(self):
        return os.path.join(
            self.get_path_for_component(self.WorkflowManagerPath.semaphore), "abort"
        )

    @staticmethod
    def create_dir_structure(path):
        WorkflowManager._create_dir_if_needed(path)

        # workflow folder
        for comp in WorkflowManager.WorkflowManagerPath:
            WorkflowManager._create_dir_if_needed(
                WorkflowManager.get_path_for_component_and_dir(path, comp)
            )

    def copy_logs_if_required(self):
        # if not self.database.progressDB.has(ProgressKeys.savedLogs):
        #     return Logger.debug(f"Workflow '{self.wid}' has copied logs, skipping")

        jobs = self.database.jobsDB.get_all()

        # iterate through all jobs
        for j in jobs:
            od = self.get_path_for_component(self.WorkflowManagerPath.logs)
            base = j.id_
            if j.shard:
                base += "shard-" + str(j.shard)
            on_base = os.path.join(od, base)

            if j.stdout and self.filescheme.exists(j.stdout):
                self.filescheme.cp_from(j.stdout, on_base + "_stdout", force=True)

            if j.stderr and self.filescheme.exists(j.stderr):
                self.filescheme.cp_from(j.stderr, on_base + "_stderr", force=True)

            if j.script and self.filescheme.exists(j.script):
                self.filescheme.cp_from(j.script, on_base + "_script", force=True)

    def set_status(
        self,
        status: TaskStatus,
        force_notification=False,
        error: Optional[str] = None,
        additional_information: Optional[str] = None,
    ):
        prev = self.database.submission_metadata.metadata.status

        if prev == status and not force_notification:
            return

        Logger.info("Status changed to: " + str(status))
        self.database.runevents.update(run_id=RunModel.DEFAULT_ID, status=status)
        self.database.submission_metadata.metadata.status = status
        if error:
            self.database.submission_metadata.metadata.error = error
        self.database.submission_metadata.save_changes()
        self.database.commit()

        # send an email here
        meta = self.database.get_metadata()
        for idx in range(1, 6):
            if meta is not None:
                break
            Logger.info(
                f"Metadata was none when getting from db, sleeping for a second then will try again {5-idx} times"
            )
            time.sleep(1)
            meta = self.database.get_metadata()

        if status.should_notify():

            # prepare recently running information
            logsdir = self.get_path_for_component(self.WorkflowManagerPath.logs)
            logpath = os.path.join(
                logsdir, f"janis-monitor.{self.submission_id}.debug.log"
            )

            additional_information = f"""
{additional_information or ""}

Log path: {logpath}
"""

            NotificationManager.notify_status_change(
                status, meta, additional_information=additional_information
            )

    def save_metadata(self, meta: RunModel) -> Optional[bool]:
        if not meta:
            return None

        self.database.save_metadata(meta)

        self.set_status(meta.status)

        return (
            meta.status in TaskStatus.final_states()
            or meta.status == TaskStatus.EXECUTION_ENDED_SUCCESSFULLY
        )

    @staticmethod
    def mark_aborted(execution_dir, wid: Optional[str]) -> bool:
        try:
            path = os.path.join(
                WorkflowManager.get_path_for_component_and_dir(
                    execution_dir, WorkflowManager.WorkflowManagerPath.semaphore
                ),
                "abort",
            )
            Logger.info(f"Aborting task '{wid}' by writing to '{path}'")
            with open(path, "w+") as f:
                f.write(f"Requesting abort {DateUtil.now()}")
            return True
        except Exception as e:
            Logger.critical("Couldn't mark aborted: " + str(e))
            return False

    def remove_semaphores(self):
        path = self.get_path_for_component(self.WorkflowManagerPath.semaphore)
        if os.path.exists(path):
            rmtree(path)

    def abort(self) -> bool:
        self.set_status(TaskStatus.ABORTING, force_notification=False)
        status = False

        engine = self.engine
        try:
            status = bool(engine.terminate_task(self.get_engine_id()))
            self.remove_semaphores()
        except Exception as e:
            Logger.critical("Couldn't abort task from engine: " + str(e))
        try:
            self.stop_engine_and_db()
        except Exception as e:
            Logger.critical("Couldn't stop engine: " + str(e))

        self.set_status(TaskStatus.ABORTED, force_notification=False)

        return status

    def suspend_workflow(self):
        try:
            # reset pause flag
            self.database.commit()
            self.database.submission_metadata.save_changes()

            self.stop_engine_and_db()
            self.set_status(TaskStatus.SUSPENDED)

            self.database.close()

            self.remove_semaphores()

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
