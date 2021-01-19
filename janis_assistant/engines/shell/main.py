import json
import os
import re
import subprocess
from typing import Dict, Any
from datetime import datetime

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
from janis_assistant.utils.dateutils import DateUtil

class ShellLogger(ProcessLogger):

    statusupdateregex = re.compile("INFO \[(.*)\] (.+)$")

    def __init__(self, sid: str, process, logfp, metadata_callback, exit_function=None, stdout_file_path: str = None):
        self.sid = sid

        self.error = None
        self.metadata_callback = metadata_callback
        self.outputs = None
        self.workflow_scope = []
        self.stdout_file_path = stdout_file_path
        self.stdout_tag_name = None
        super().__init__(
            process=process, prefix="shell", logfp=logfp, exit_function=exit_function
        )

    def run(self):
        self.outputs = []
        try:
            # Handle stderr first
            for c in iter(
                self.process.stderr.readline, "b"
            ):
                if self.should_terminate:
                    return

                line = c.decode("utf-8").rstrip()

                if not line:
                    rc = self.process.poll()
                    if rc is not None:
                        # process has terminated
                        self.rc = rc
                        break

                has_error = self.error_keyword and self.error_keyword in line
                # log to debug / critical the self.prefix + line
                (Logger.critical if has_error else Logger.debug)(self.prefix + " " + line)

                if has_error:
                    # process has terminated
                    rc = self.process.poll()
                    self.rc = rc
                    self.logfp.flush()
                    os.fsync(self.logfp.fileno())

                    print("Process has ended")
                    if self.exit_function:
                        self.exit_function(rc)
                    return

            # Now, look at stdout
            for c in iter(
                self.process.stdout.readline, "b"
            ):
                if self.should_terminate:
                    return

                line = c.decode("utf-8")

                # Reading the Tool output tag name printed in stdout
                start_marker = "STDOUT START: "
                if line.startswith(start_marker):
                    self.stdout_tag_name = line.rstrip().split(start_marker)[1]
                else:
                    self.outputs.append(line)

                line = line.rstrip()
                if not line:
                    rc = self.process.poll()
                    if rc is not None:
                        # process has terminated
                        self.rc = rc
                        break

                should_write = (datetime.now() - self.last_write).total_seconds() > 5

                if self.logfp and not self.logfp.closed:
                    self.logfp.write(line + "\n")
                    if should_write:
                        self.last_write = datetime.now()
                        self.logfp.flush()
                        os.fsync(self.logfp.fileno())

            Logger.info("Process has completed")

            self.terminate()
            if self.exit_function:
                self.exit_function(self)

        except KeyboardInterrupt:
            self.should_terminate = True
            print("Detected keyboard interrupt")
            # raise
        except Exception as e:
            print("Detected another error")
            raise e


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
        # # we're going to abort!
        # if self.process_id:
        #     Logger.info("Received stop_engine request for Shell")
        #     try:
        #         import signal
        #
        #         os.kill(self.process_id, signal.SIGTERM)
        #     except Exception as e:
        #         Logger.critical("Couldn't terminate Shell as " + str(e))
        #
        # else:
        #     Logger.critical("Couldn't terminate Shell as there was no process ID")

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
        Logger.info(f"Running command: {cmd}")

        stdout_file_path = os.path.join(self.execution_dir, "_stdout")
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE,
        )

        Logger.info("Shell has started with pid=" + str(process.pid))
        self.process_id = process.pid

        self._logger = ShellLogger(
            wid,
            process,
            logfp=open(self.logfile, "a+"),
            metadata_callback=self.task_did_update,
            exit_function=self.task_did_exit,
            stdout_file_path=stdout_file_path
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

    @staticmethod
    def process_potential_out(run_id, key, out):

        if isinstance(out, list):
            outs = [Shell.process_potential_out(run_id, key, o) for o in out]
            ups = {}
            for o in outs:
                for k, v in o.items():
                    if k not in ups:
                        ups[k] = []
                    ups[k].append(v)
            return ups

        updates = {}
        if out is None:
            return {}

        if is_python_primitive(out):
            updates[key] = WorkflowOutputModel(
                submission_id=None,
                run_id=run_id,
                id_=key,
                original_path=None,
                is_copyable=False,
                timestamp=DateUtil.now(),
                value=out,
                new_path=None,
                output_folder=None,
                output_name=None,
                secondaries=None,
                extension=None,
            )

        elif "path" in out:
            updates[key] = WorkflowOutputModel(
                submission_id=None,
                run_id=run_id,
                id_=key,
                is_copyable=True,
                value=out["path"],
                original_path=None,
                timestamp=DateUtil.now(),
                new_path=None,
                output_folder=None,
                output_name=None,
                secondaries=None,
                extension=None,
            )
            for s in out.get("secondaryFiles", []):
                path = s["path"]
                ext = path.rpartition(".")[-1]
                newk = f"{key}_{ext}"
                updates[newk] = WorkflowOutputModel(
                    submission_id=None,
                    run_id=run_id,
                    id_=newk,
                    value=path,
                    original_path=None,
                    is_copyable=True,
                    timestamp=DateUtil.now(),
                    new_path=None,
                    output_folder=None,
                    output_name=None,
                    secondaries=None,
                    extension=None,
                )

        return updates

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
            jobs=list(self.taskmeta.get("jobs", {}).values()),
            error=self.taskmeta.get("error"),
        )

    def task_did_exit(self, logger: ShellLogger):
        Logger.debug("Shell fired 'did exit'")
        self.taskmeta["status"] = TaskStatus.COMPLETED
        self.taskmeta["finish"] = DateUtil.now()

        with open(logger.stdout_file_path, "w") as f:
            f.writelines(logger.outputs)

        if logger.stdout_tag_name:
            self.taskmeta["outputs"] = {logger.stdout_tag_name: logger.stdout_file_path}

        # if status != TaskStatus.COMPLETED:
        #     js: Dict[str, RunJobModel] = self.taskmeta.get("jobs")
        #     for j in js.values():
        #         if j.status != TaskStatus.COMPLETED:
        #             j.status = status

        # if logger.error:
        #     self.taskmeta["error"] = logger.error

        Logger.debug(self.progress_callbacks)
        for callback in self.progress_callbacks.get(self._logger.sid, []):
            callback(self.metadata(self._logger.sid))

    def task_did_update(self, logger: ShellLogger, job: RunJobModel):
        Logger.debug(f"Updated task {job.id_} with status={job.status}")
        self.taskmeta["jobs"][job.id_] = job

        for callback in self.progress_callbacks.get(logger.sid, []):
            callback(self.metadata(logger.sid))