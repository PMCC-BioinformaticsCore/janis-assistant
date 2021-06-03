import json
import os
import glob
import time
import re
import subprocess
import socketserver
from typing import Dict, Any, Optional
from datetime import datetime
from http.server import BaseHTTPRequestHandler

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


def make_request_handler(nextflow_logger):
    class NextflowRequestHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len)
            body_as_str = post_body.decode("utf-8")
            body_as_json = json.loads(body_as_str)

            # Logger.debug(body_as_json)

            event = body_as_json["event"]

            if event == "completed" or event == "error":
                # Logger.debug("shutting down server")
                self.server.shutdown()
                # Logger.debug("server shut down")
                nextflow_logger.exit_function(nextflow_logger)
                nextflow_logger.terminate()
            elif event.startswith("process_"):
                trace = body_as_json["trace"]
                name = trace["name"]
                process = trace["process"]
                task_id = trace["task_id"]
                work_dir = trace["workdir"]
                status = trace["status"]
                exit_code = trace["exit"]

                janis_status = TaskStatus.QUEUED
                if status == "COMPLETED":
                    janis_status = TaskStatus.COMPLETED
                elif status == "RUNNING":
                    janis_status = TaskStatus.RUNNING
                elif status == "SUBMITTED":
                    janis_status = TaskStatus.QUEUED
                elif status == "FAILED":
                    janis_status = TaskStatus.FAILED

                start = DateUtil.now() if janis_status == TaskStatus.RUNNING else None
                finish = DateUtil.now() if janis_status == TaskStatus.COMPLETED else None

                parentid = None
                # parts = name.split(":")
                # if len(parts) >= 2:
                #     parentid = parts[0]

                if process == NextflowTranslator.FINAL_STEP_NAME:
                    nextflow_logger.nf_monitor = NextFlowTaskMonitor(id=task_id, name=name, status=janis_status,
                                                                     exit=exit_code, work_dir=work_dir)
                else:
                    job = RunJobModel(
                        submission_id=None,
                        run_id=nextflow_logger.sid,
                        id_=name,
                        parent=parentid,
                        name=name,
                        status=janis_status,
                        start=start,
                        finish=finish,
                        # backend=self.executor,
                        workdir=work_dir
                    )

                    nextflow_logger.metadata_callback(nextflow_logger, job)

    return NextflowRequestHandler


class NextflowLogger(ProcessLogger):

    def __init__(self, sid: str, process, nextflow_log_filename, logfp, metadata_callback, execution_directory, exit_function=None):
        self.sid = sid

        self.error = None
        self.metadata_callback = metadata_callback
        self.outputs = None
        self.workflow_scope = []
        self.execution_directory = execution_directory
        self.work_directory = None
        # self.nextflow_log_file = os.path.join(self.execution_directory, ".nextflow.log")
        self.nextflow_log_file = os.path.join(self.execution_directory, nextflow_log_filename)
        self.nf_monitor = None
        self.executor = None
        self.current_nf_monitor = None
        self.current_nf_submitter = None
        super().__init__(
            process=process, prefix="nextflow: ", logfp=logfp, exit_function=exit_function
        )

    def run(self):
        HOST = "localhost"
        PORT = 8000

        try:
            httpd = socketserver.ThreadingTCPServer((HOST, PORT), make_request_handler(self))
            print("serving at port", PORT)
            httpd.serve_forever()

            # self.read_script_output()
            # self.read_log()
            # self.exit_function(self)
            # self.terminate()

        except KeyboardInterrupt:
            self.should_terminate = True
            print("Detected keyboard interrupt")
            httpd.shutdown()
            # raise
        except Exception as e:
            print("Detected another error")
            httpd.shutdown()
            raise e

    def read_script_output(self):
        for c in iter(
                self.process.stdout.readline, ""
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
                # else:
                #     time.sleep(1)


    def read_important_value(self, line: str):
        """
        Nextflow log lines prints the important value of the log line after " > "

        :param line:
        :type line:
        :return:
        :rtype:
        """
        # example line: "nextflow.executor.Executor - [warm up] executor > local"
        regex = r".+ > (.+)"
        match = re.search(regex, line)

        if match and len(match.groups()) >= 1:
            return match[1]

        return None

    def read_process_work_dir(self, line: str):
        regex = r"(.+)\[(.+)\](.+)"
        match = re.search(regex, line)

        if match and len(match.groups()) >= 2:
            return match[2]

        return None

    def read_executor(self, line: str):
        keyword = "nextflow.executor.Executor -"

        if keyword in line:
            self.executor = self.read_important_value(line)
            return self.executor

        return None

    def read_work_dir(self, line: str):
        keyword = "nextflow.Session - Work-dir: "
        regex = rf"(.+)({keyword})(.+) \[(.+)\]"
        if keyword in line:
            match = re.search(regex, line)
            Logger.debug(match.groups())

            if match and len(match.groups()) >= 3:
                self.work_directory = match[3]

        return None

    def read_task_monitor(self, line: str):
        self.current_nf_monitor = None
        if "[Task monitor]" and "TaskHandler[" in line:
            self.current_nf_monitor = NextFlowTaskMonitor.from_task_monitor(line)
        elif "[Task submitter]" in line and "Submitted process > " in line:
            process_name = self.read_important_value(line)
            self.current_nf_monitor = NextFlowTaskMonitor(name=process_name, status=TaskStatus.RUNNING)
        elif "nextflow.processor.TaskProcessor - Starting process " in line:
            process_name = self.read_important_value(line)
            self.current_nf_monitor = NextFlowTaskMonitor(name=process_name, status=TaskStatus.QUEUED)
        elif "nextflow.processor.TaskProcessor - " in line and "Cached process > " in line:
            process_name = self.read_important_value(line)
            process_work_dir = self.read_process_work_dir(line)
            Logger.debug(process_work_dir)

            work_dir_path = None
            if process_work_dir is not None:
                work_dir_path = os.path.join(self.work_directory, f"{process_work_dir}*")

            self.current_nf_monitor = NextFlowTaskMonitor(name=process_name, status=TaskStatus.COMPLETED, work_dir=work_dir_path)

        return self.current_nf_monitor

    def poll_process_update(self):
        if self.current_nf_monitor is not None and \
                self.current_nf_monitor.name is not None and \
                self.current_nf_monitor.status is not None:

            if not self.current_nf_monitor.name.startswith(NextflowTranslator.FINAL_STEP_NAME):
                parentid = None
                start = DateUtil.now() if self.current_nf_monitor.status == TaskStatus.RUNNING else None
                finish = DateUtil.now() if self.current_nf_monitor.status == TaskStatus.COMPLETED else None

                job = RunJobModel(
                    submission_id=None,
                    run_id=self.sid,
                    id_=self.current_nf_monitor.name,
                    parent=parentid,
                    name=self.current_nf_monitor.name,
                    status=self.current_nf_monitor.status,
                    start=start,
                    finish=finish,
                    backend=self.executor,
                    workdir=self.current_nf_monitor.workDir)

                self.metadata_callback(self, job)
            else:
                self.nf_monitor = self.current_nf_monitor

    def read_log(self):
        num_tries = 10
        while True and num_tries > 0:
            Logger.info("Waiting for Nextflow to start logging")
            if os.path.exists(self.nextflow_log_file):
                break
            time.sleep(2)
            num_tries -= 1

        if not os.path.exists(self.nextflow_log_file):
            raise Exception(f"{self.nextflow_log_file} not found")

        with open(self.nextflow_log_file, "r") as f:
            Logger.info(f"Start reading Nextflow log file in {self.nextflow_log_file}")
            try:
                while True:
                    line = f.readline()
                    if not line:
                        rc = self.process.poll()
                        if rc is not None:
                            # process has terminated
                            Logger.debug("process has terminated")
                            self.rc = rc
                            break
                        else:
                            time.sleep(1)
                        continue

                    self.read_executor(line)
                    self.read_work_dir(line)
                    self.read_task_monitor(line)

                    self.poll_process_update()

                    should_write = (datetime.now() - self.last_write).total_seconds() > 5

                    if self.logfp and not self.logfp.closed:
                        self.logfp.write(line + "\n")
                        if should_write:
                            self.last_write = datetime.now()
                            self.logfp.flush()
                            os.fsync(self.logfp.fileno())

            except Exception as e:
                if self.logfp and not self.logfp.closed:
                    self.logfp.write(line + "\n")
                    self.logfp.flush()


class NextFlowTaskMonitor:
    task_monitor_regex = r"TaskHandler\[(.+)\]"

    def __init__(self, id: Optional[str] = None,
                 name: Optional[str] = None,
                 status: Optional[str] = None,
                 exit: Optional[str] = None,
                 error: Optional[str] = None,
                 work_dir: Optional[str] = None,
                 stdout_path: Optional[str] = None,
                 stderr_path: Optional[str] = None,
                 ):

        self.id = id
        self.name = name
        self.status = status
        self.exit = exit
        self.error = error
        self.workDir = work_dir
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path

    @classmethod
    def from_task_monitor(cls, task_monitor_log_entry: str):

        Logger.info("[Task monitor]")
        Logger.info(task_monitor_log_entry)
        match = re.search(cls.task_monitor_regex, task_monitor_log_entry)
        monitor = cls()

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
                    setattr(monitor, key.strip(), val.strip())

                else:
                    Logger.warn(f"unknown task_monitor entry {att}")

            # TODO: why is nextflow returning
            # 'workDir: /data/gpfs/projects/punim0755/workspace/juny/nextflow/fastqcNf/janis/execution/work/f1/da15bd54d649281b1f60e93047128f started: 1618904643195'
            if monitor.workDir is not None:
                unexpected_str = " started"
                if monitor.workDir.endswith(unexpected_str):
                    monitor.workDir = monitor.workDir[:- len(unexpected_str)]

        if monitor:
            monitor.read_task_status()
            monitor.read_stdout_path()
            monitor.read_stderr_path()

        return monitor

    # def init_attributes(self):
    #     self.id = None
    #     self.name = None
    #     self.status = None
    #     self.exit = None
    #     self.error = None
    #     self.workDir = None
    #     self.stdout_path = None

    def read_task_status(self):
        if self.status == 'COMPLETED' and self.exit == '0':
            self.status = TaskStatus.COMPLETED
        elif self.status == 'COMPLETED' and self.exit == '1':
            self.status = TaskStatus.FAILED
        else:
            self.status = TaskStatus.PROCESSING

    def read_stdout_path(self):
        self.stdout_path = os.path.join(self.workDir, ".command.out")

    def read_stderr_path(self):
        self.stderr_path = os.path.join(self.workDir, ".command.err")


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

        cmd = self.config.build_command_line(source_path=source_path, input_path=input_path,
                                             nextflow_log_filename=self.nextflow_log_filename)

        Logger.info(f"Running command: {cmd}")

        process = subprocess.Popen(
            cmd,  preexec_fn=os.setsid,
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

        # output_meta_path = os.path.join(self.taskmeta["work_directory"], NextflowTranslator.OUTPUT_METADATA_FILENAME)
        output_meta_path = None
        output_meta_regex = os.path.join(self.taskmeta["work_directory"], NextflowTranslator.OUTPUT_METADATA_FILENAME)
        Logger.debug(output_meta_regex)
        found = glob.glob(output_meta_regex)
        Logger.debug(found)
        if len(found) == 1:
            output_meta_path = found[0]
        else:
            raise Exception(f"Cannot find file that matches {output_meta_regex}")

        if output_meta_path is not None:
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