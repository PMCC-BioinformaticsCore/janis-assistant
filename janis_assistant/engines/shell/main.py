import json
import os
import re
import subprocess
from typing import Dict, Any

from janis_core import LogLevel
from janis_core.types.data_types import is_python_primitive
from janis_core.utils.logger import Logger
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.run import RunModel
from janis_assistant.data.models.workflowjob import RunJobModel
from janis_assistant.engines.shell.shellconfiguration import ShellConfiguration
from janis_assistant.engines.engine import Engine, TaskStatus
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.utils import ProcessLogger
from janis_assistant.utils.dateutil import DateUtil

class ShellLogger(ProcessLogger):

    statusupdateregex = re.compile("INFO \[(.*)\] (.+)$")

    def __init__(self, sid: str, process, logfp, metadata_callback, exit_function=None):
        self.sid = sid

        self.error = None
        self.metadata_callback = metadata_callback
        self.outputs = None
        self.workflow_scope = []
        super().__init__(
            process=process, prefix="shell", logfp=logfp, exit_function=exit_function
        )


class Shell(Engine):
    def __init__(
        self,
        execution_dir: str,
        logfile=None,
        identifier: str = "shell",
        config: ShellConfiguration = None,
    ):
        super().__init__(
            identifier, EngineType.shell, logfile=logfile, execution_dir=execution_dir
        )
        self.process = None
        self._logger = None

        self.taskmeta = {}

        self.config = None

    def start_engine(self):
        Logger.log(
            "Shell doesn't run in a server mode, an instance will "
            "automatically be started when a task is created"
        )
        return self

    def stop_engine(self):
        # we're going to abort!
        if self.process_id:
            Logger.info("Received stop_engine request for Shell")
            try:
                import signal

                os.kill(self.process_id, signal.SIGTERM)
            except Exception as e:
                Logger.critical("Couldn't terminate Shell as " + str(e))

        else:
            Logger.critical("Couldn't terminate Shell as there was no process ID")

        return self

    def start_from_paths(self, wid, source_path: str, input_path: str, deps_path: str):
        Logger.debug(f"source_path: {source_path}")
        Logger.debug(f"input_path: {input_path}")
        Logger.debug(f"deps_path: {deps_path}")

        self.taskmeta = {
            "start": DateUtil.now(),
            "status": TaskStatus.PROCESSING,
            "jobs": {},
        }

        cmd = ["sh", source_path, input_path]
        # cmd = f"source {input_path}; sh {source_path};"
        Logger.info(f"Running command: {cmd}")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE,
        )
        Logger.debug(f"returncode {process.returncode}")
        Logger.debug(f"stdout {process.stdout.read().decode()}")
        Logger.debug(f"stderr {process.stderr.read().decode()}")

        Logger.info("Shell has started with pid=" + str(process.pid))
        self.process_id = process.pid

        self._logger = ShellLogger(
            wid,
            process,
            logfp=open(self.logfile, "a+"),
            metadata_callback=self.task_did_update,
            exit_function=self.task_did_exit,
        )

        return wid

    def poll_task(self, identifier) -> TaskStatus:
        return self.taskmeta.get("status", TaskStatus.PROCESSING)

    def outputs_task(self, identifier) -> Dict[str, Any]:
        outs = self.taskmeta.get("outputs")

        if not outs:
            return {}

        retval: Dict[str, WorkflowOutputModel] = {}
        for k, o in outs.items():
            retval.update(self.process_potential_out(identifier, k, o))

        return retval

    def terminate_task(self, identifier) -> TaskStatus:
        self.stop_engine()
        self.taskmeta["status"] = TaskStatus.ABORTED
        return TaskStatus.ABORTED

    def metadata(self, identifier) -> RunModel:
        return RunModel(
            id_=identifier,
            engine_id=identifier,
            execution_dir=None,
            submission_id=None,
            name=identifier,
            status=self.taskmeta.get("status"),
            # start=self.taskmeta.get("start"),
            # finish=self.taskmeta.get("finish"),
            # outputs=meta.get("outputs") or [],
            jobs=list(self.taskmeta.get("jobs", {}).values()),
            error=self.taskmeta.get("error"),
            # executiondir=None,
        )

    def task_did_exit(self, logger: ShellLogger):
        Logger.debug("Shell fired 'did exit'")
        Logger.debug(logger)
        # self.taskmeta["status"] = status
        self.taskmeta["finish"] = DateUtil.now()
        # self.taskmeta["outputs"] = logger.outputs

        # if status != TaskStatus.COMPLETED:
        #     js: Dict[str, RunJobModel] = self.taskmeta.get("jobs")
        #     for j in js.values():
        #         if j.status != TaskStatus.COMPLETED:
        #             j.status = status

        # if logger.error:
        #     self.taskmeta["error"] = logger.error

        # for callback in self.progress_callbacks.get(logger.sid, []):
        #     callback(self.metadata(logger.sid))

    def task_did_update(self, logger: ShellLogger, job: RunJobModel):
        Logger.debug(f"Updated task {job.id_} with status={job.status}")
        self.taskmeta["jobs"][job.id_] = job

        for callback in self.progress_callbacks.get(logger.sid, []):
            callback(self.metadata(logger.sid))