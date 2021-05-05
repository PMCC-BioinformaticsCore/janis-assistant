import json
import os
import time
import re
import subprocess
from typing import Dict, Any
from datetime import datetime

from janis_core import LogLevel
from janis_core.types.data_types import is_python_primitive
from janis_core.utils.logger import Logger
from janis_core.translations import nfgen, NextflowTranslator
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.run import RunModel
from janis_assistant.data.models.workflowjob import RunJobModel
from janis_assistant.engines.nextflow.nextflowconfiguration import NextflowConfiguration
from janis_assistant.engines.engine import Engine, TaskStatus
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.utils import ProcessLogger
from janis_assistant.utils.dateutils import DateUtil


class NextflowLogger(ProcessLogger):

    monitor_tag = "[Task monitor]"
    submitter_tag = "[Task submitter]"

    def __init__(self, sid: str, process, nextflow_log_filename, logfp, metadata_callback, execution_directory, exit_function=None):
        self.sid = sid

        self.error = None
        self.metadata_callback = metadata_callback
        self.outputs = None
        self.workflow_scope = []
        self.execution_directory = execution_directory
        # self.nextflow_log_file = os.path.join(self.execution_directory, ".nextflow.log")
        self.nextflow_log_file = os.path.join(self.execution_directory, nextflow_log_filename)
        self.nf_monitor = None
        super().__init__(
            process=process, prefix="nextflow: ", logfp=logfp, exit_function=exit_function
        )

    def run(self):

        try:
            # self.read_script_output()
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
                self.process.stdout.readline, ""
        ):
            if self.should_terminate:
                return

            line = c.decode("utf-8").rstrip()
            Logger.debug(line)

            # if "executor >" in line:
            #     self.read_log()


            # if not line:
            #     rc = self.process.poll()
            #     if rc is not None:
            #         # process has terminated
            #         self.rc = rc
            #         break
            #
            # should_write = (datetime.now() - self.last_write).total_seconds() > 5
            #
            # if self.logfp and not self.logfp.closed:
            #     self.logfp.write(line + "\n")
            #     if should_write:
            #         self.last_write = datetime.now()
            #         self.logfp.flush()
            #         os.fsync(self.logfp.fileno())

    def read_log(self):
        num_tries = 5
        while True and num_tries > 0:
            if os.path.exists(self.nextflow_log_file):
                break
            time.sleep(2)
            num_tries -= 1

        with open(self.nextflow_log_file, "r") as f:
            Logger.info("reading nextflow log file")
            while True:
                line = f.readline()
                if not line:
                    rc = self.process.poll()
                    if rc is not None:
                        # process has terminated
                        Logger.debug("process has terminated")
                        self.rc = rc
                        break
                    continue

                if self.should_terminate:
                    return

                current_nf_monitor = None
                if self.monitor_tag in line:
                   current_nf_monitor = NextFlowTaskMonitor(line)

                current_nf_submitter = None
                if self.submitter_tag in line:
                    current_nf_submitter = NextFlowTaskSubmitter(line)

                if current_nf_submitter is not None:
                    if current_nf_submitter.name is not None and current_nf_submitter.status is not None \
                            and not current_nf_submitter.name.startswith(NextflowTranslator.FINAL_STEP_NAME):
                        jid = current_nf_submitter.name
                        parentid = None
                        stepname = current_nf_submitter.name
                        status = current_nf_submitter.status
                        start = DateUtil.now() if current_nf_submitter.status == TaskStatus.RUNNING else None
                        finish = DateUtil.now() if current_nf_submitter.status == TaskStatus.COMPLETED else None

                        job = RunJobModel(
                            submission_id=None,
                            run_id=self.sid,
                            id_=jid,
                            parent=parentid,
                            name=stepname,
                            status=status,
                            start=start,
                            finish=finish,
                            backend="local",
                            batchid="",
                            stderr=self.logfp.name,
                        )

                        self.metadata_callback(self, job)

                if current_nf_monitor is not None and current_nf_monitor.name is not None:
                    if current_nf_monitor.name.startswith(NextflowTranslator.FINAL_STEP_NAME):
                        self.nf_monitor = current_nf_monitor
                    else:
                        jid = current_nf_monitor.name
                        parentid = None
                        stepname = current_nf_monitor.name
                        status = current_nf_monitor.status
                        start = DateUtil.now() if current_nf_monitor.status == TaskStatus.RUNNING else None
                        finish = DateUtil.now() if current_nf_monitor.status == TaskStatus.COMPLETED else None

                        job = RunJobModel(
                            submission_id=None,
                            run_id=self.sid,
                            id_=jid,
                            parent=parentid,
                            name=stepname,
                            status=status,
                            start=start,
                            finish=finish,
                            backend="local",
                            batchid="",
                            stderr=self.logfp.name,
                        )

                        self.metadata_callback(self, job)

                # line = c.decode("utf-8").rstrip()

                # If we find any json line, then it is the output
                try:
                    self.outputs = json.loads(line)
                except Exception as e:
                    pass

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

        Logger.debug("[Task monitor]")
        Logger.debug(task_monitor_log_entry)
        task_monitor = []
        match = re.search(self.task_monitor_regex, task_monitor_log_entry)
        if match and len(match.groups()) >= 1:
            task_monitor_str = match[1]
            task_monitor = task_monitor_str.split("; ")

        Logger.debug("task_monitor")
        Logger.debug(task_monitor)

        for att in task_monitor:
            if ": " in att:
                parts = att.split(": ")
                key = parts[0]
                val = parts[1]
                setattr(self, key.strip(), val.strip())

                # TODO: why is nextflow returning
                # 'workDir: /data/gpfs/projects/punim0755/workspace/juny/nextflow/fastqcNf/janis/execution/work/f1/da15bd54d649281b1f60e93047128f started: 1618904643195'
                if hasattr(self, "workDir"):
                    unexpected_str = " started"
                    if self.workDir.endswith(unexpected_str):
                        self.workDir = self.workDir[:- len(unexpected_str)]

            else:
                Logger.warn(f"unknown task_monitor entry {att}")

        if task_monitor:
            self.read_task_status()
            self.read_stdout_path()
        else:
            self.init_attributes()

    def init_attributes(self):
        self.id = None
        self.name = None
        self.status = None
        self.exit = None
        self.error = None
        self.workDir = None
        self.stdout_path = None

    def read_task_status(self):
        if self.status == 'COMPLETED' and self.exit == '0':
            self.status = TaskStatus.COMPLETED
        elif self.status == 'COMPLETED' and self.exit == '1':
            self.status = TaskStatus.FAILED
        else:
            self.status = TaskStatus.PROCESSING

    def read_stdout_path(self):
        self.stdout_path = os.path.join(self.workDir, ".command.out")


class NextFlowTaskSubmitter:
    INFO = "INFO"
    task_submitter_regex = r" > (.+)"

    def __init__(self, task_submitter_log_entry: str):
        self.init_attributes()

        if self.INFO in task_submitter_log_entry:
            if "Submitted process" in task_submitter_log_entry:
                self.status = TaskStatus.RUNNING

            match = re.search(self.task_submitter_regex, task_submitter_log_entry)
            if match and len(match.groups()) >= 1:
                self.name = match[1]

    def init_attributes(self):
        self.name = None
        self.status = None


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
        self.nextflow_log_filename = f"nextflow-{int(time.time())}.log"

        self.taskmeta = {}

        self.find_or_generate_config(config)

    def find_or_generate_config(self, config: NextflowConfiguration):
        from janis_assistant.data.models.preparedjob import PreparedJob

        job = PreparedJob.instance()

        if config:
            self.config = config
        else:
            self.config = (
                job.template.template.engine_config(EngineType.nextflow, job)
                or NextflowConfiguration()
            )

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
        config_path = os.path.join(tool_dir, nfgen.CONFIG_FILENAME)
        # cmd = ["nextflow", "-C", config_path, "run", source_path, '-params-file', input_path]

        # nf_config = NextflowConfiguration()

        cmd = self.config.build_command_line(source_path=source_path, input_path=input_path,
                                             nextflow_log_filename=self.nextflow_log_filename)

        Logger.info(f"Running command: {cmd}")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE,
        )

        Logger.info("Nextflow has started with pid=" + str(process.pid))
        self.process_id = process.pid

        self._logger = NextflowLogger(
            wid,
            process,
            nextflow_log_filename=self.nextflow_log_filename,
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

        if self.taskmeta["work_directory"] is None:
            return outputs

        output_meta_path = os.path.join(self.taskmeta["work_directory"], NextflowTranslator.OUTPUT_METADATA_FILENAME)
        with open(output_meta_path, "r") as f:
            for line in f:
                try:
                    key, val = line.split("=")
                    key = key.strip()
                    val = val.strip()

                    # Note: cannot use json.load or ast.literal_eval because strings are not quoted
                    if val == "[]":
                        val = []
                    elif val.startswith("["):
                        val = val.strip("][").split(", ")

                    outputs[key] = val

                except Exception as e:
                    raise Exception(f"Failed to parse line {line} in {NextflowTranslator.OUTPUT_METADATA_FILENAME}")

        return outputs

    def task_did_exit(self, logger: NextflowLogger):
        Logger.debug("Shell fired 'did exit'")

        if logger.nf_monitor is not None:
            self.taskmeta["status"] = logger.nf_monitor.status
            self.taskmeta["finish"] = DateUtil.now()
            self.taskmeta["work_directory"] = logger.nf_monitor.workDir
            self.taskmeta["stdout_path"] = logger.nf_monitor.stdout_path
            self.taskmeta["outputs"] = self.read_janis_output()
        else:
            self.taskmeta["status"] = TaskStatus.FAILED
            self.taskmeta["outputs"] = None

        for callback in self.progress_callbacks.get(self._logger.sid, []):
            callback(self.metadata(self._logger.sid))

    def task_did_update(self, logger: NextflowLogger, job: RunJobModel):
        Logger.debug(f"Updated task {job.id_} with status={job.status}")
        self.taskmeta["jobs"][job.id_] = job

        for callback in self.progress_callbacks.get(logger.sid, []):
            callback(self.metadata(logger.sid))