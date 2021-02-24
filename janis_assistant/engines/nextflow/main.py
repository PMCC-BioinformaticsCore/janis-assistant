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
from janis_assistant.engines.nextflow.nextflowconfiguration import NextflowConfiguration
from janis_assistant.engines.engine import Engine, TaskStatus
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.utils import ProcessLogger
from janis_assistant.utils.dateutils import DateUtil


class NextflowLogger(ProcessLogger):

    error_keywords = ["error", "fail", "exception"]
    monitor_tag = "[Task monitor]"

    def __init__(self, sid: str, process, logfp, metadata_callback, execution_directory, exit_function=None):
        self.sid = sid

        self.error = None
        self.metadata_callback = metadata_callback
        self.outputs = None
        self.workflow_scope = []
        self.execution_directory = execution_directory
        self.nextflow_log_file = os.path.join(self.execution_directory, ".nextflow.log")
        self.nf_monitor = None
        super().__init__(
            process=process, prefix="nextflow: ", logfp=logfp, exit_function=exit_function
        )

    def run(self):

        try:
            self.read_script_output()
            self.read_log()

            self.exit_function(self)
            self.terminate()

        except KeyboardInterrupt:
            self.should_terminate = True
            print("Detected keyboard interrupt")
            # raise
        except Exception as e:
            print("Detected another error")
            raise e

    def read_script_output(self):
        for c in iter(
                self.process.stdout.readline, "b"
        ):
            if self.should_terminate:
                return

            line = c.decode("utf-8").rstrip()

            Logger.debug(line)

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

    def read_log(self):

        with open(self.nextflow_log_file, "r") as f:
            for line in f:
                if self.should_terminate:
                    return

                if self.monitor_tag in line:
                    self.nf_monitor = NextFlowTaskMonitor(line)

                # line = c.decode("utf-8").rstrip()

                # If we find any json line, then it is the output
                try:
                    self.outputs = json.loads(line)
                except Exception as e:
                    pass

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

class NextFlowTaskMonitor:
    task_monitor_regex = r"TaskHandler\[(.+)\]"

    def __init__(self, task_monitor_log_entry: str):

        match = re.search(self.task_monitor_regex, task_monitor_log_entry)
        if len(match.groups()) >= 1:
            task_monitor_str = match[1]
        task_monitor = task_monitor_str.split("; ")

        for att in task_monitor:
            key, val = att.split(": ")
            setattr(self, key.strip(), val.strip())

        self.read_task_status()
        self.read_stdout_path()

    def read_task_status(self):
        if self.status == 'COMPLETED':
            self.status = TaskStatus.COMPLETED
        else:
            self.status = TaskStatus.PROCESSING

    def read_stdout_path(self):
        self.stdout_path = os.path.join(self.workDir, ".command.out")


class Nextflow(Engine):
    def __init__(
            self,
            execution_dir: str,
            logfile=None,
            identifier: str = "nextflow",
            config: NextflowConfiguration = None,
    ):
        super().__init__(
            identifier, EngineType.nextflow, logfile=logfile, execution_dir=execution_dir
        )
        self.process = None
        self._logger = None

        self.taskmeta = {}

        self.config = None

    def start_engine(self):
        Logger.log(
            "Nextflow doesn't run in a server mode, an instance will "
            "automatically be started when a task is created"
        )
        return self

    def stop_engine(self):
        return self

    def terminate_task(self, identifier) -> TaskStatus:
        self.stop_engine()
        self.taskmeta["status"] = TaskStatus.ABORTED
        return TaskStatus.ABORTED

    def start_from_paths(self, wid, source_path: str, input_path: str, deps_path: str):
        Logger.debug(f"source_path: {source_path}")
        Logger.debug(f"input_path: {input_path}")
        Logger.debug(f"deps_path: {deps_path}")

        self.taskmeta = {
            "start": DateUtil.now(),
            "status": TaskStatus.PROCESSING,
            "jobs": {},
        }

        tool_dir = deps_path[:-1 * len(".zip")]
        workflow_dir = os.path.dirname(source_path)
        cmd = ["nextflow", "run", source_path, '-params-file', input_path]
        Logger.info(f"Running command: {cmd}")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE,
        )

        Logger.info("Nextflow has started with pid=" + str(process.pid))
        self.process_id = process.pid

        self._logger = NextflowLogger(
            wid,
            process,
            logfp=open(self.logfile, "a+"),
            metadata_callback=self.task_did_update,
            execution_directory=self.execution_dir,
            exit_function=self.task_did_exit,
        )

        return wid

    def metadata(self, identifier) -> RunModel:
        return RunModel(
            id_=identifier,
            engine_id=identifier,
            execution_dir=self.execution_dir,
            submission_id=None,
            name=identifier,
            status=self.taskmeta.get("status"),
            jobs=list(self.taskmeta.get("jobs", {}).values()),
            error=self.taskmeta.get("error"),
        )

    def outputs_task(self, identifier) -> Dict[str, Any]:
        outs = self.taskmeta.get("outputs")
        Logger.debug("outputs")
        Logger.debug(outs)

        if not outs:
            return {}

        retval: Dict[str, WorkflowOutputModel] = {}
        for k, o in outs.items():
            retval.update(self.process_potential_out(identifier, k, o))

        return retval

    @classmethod
    def process_potential_out(cls, run_id, key, out):
        if isinstance(out, list):
            outs = [cls.process_potential_out(run_id, key, o) for o in out]
            ups = {}
            for o in outs:
                for k, v in o.items():
                    if k not in ups:
                        ups[k] = []
                    ups[k].append(v)
            return ups

        if out is None:
            return {}

        return {
            key: WorkflowOutputModel(
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
        }

    def poll_task(self, identifier) -> TaskStatus:
        return self.taskmeta.get("status", TaskStatus.PROCESSING)

    def read_janis_output(self):
        outputs = {}
        output_meta_path = os.path.join(self.taskmeta["work_directory"], ".janis.out")
        with open(output_meta_path, "r") as f:
            for line in f:
                outputs = json.loads(line)

        return outputs

    def task_did_exit(self, logger: NextflowLogger):
        Logger.debug("Shell fired 'did exit'")
        Logger.debug(logger.nf_monitor.status)
        self.taskmeta["status"] = logger.nf_monitor.status
        self.taskmeta["finish"] = DateUtil.now()
        self.taskmeta["work_directory"] = logger.nf_monitor.workDir
        self.taskmeta["stdout_path"] = logger.nf_monitor.stdout_path
        self.taskmeta["outputs"] = self.read_janis_output()

        for callback in self.progress_callbacks.get(self._logger.sid, []):
            callback(self.metadata(self._logger.sid))

    def task_did_update(self, logger: NextflowLogger, job: RunJobModel):
        Logger.debug(f"Updated task {job.id_} with status={job.status}")
        self.taskmeta["jobs"][job.id_] = job

        for callback in self.progress_callbacks.get(logger.sid, []):
            callback(self.metadata(logger.sid))