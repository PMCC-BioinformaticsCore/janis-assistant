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

        cmd = ["/bin/sh", source_path]
        Logger.info(f"Running command: {cmd}")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE
        )
        Logger.info("Shell has started with pid=" + str(process.pid))
        self.process_id = process.pid

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
        pass
