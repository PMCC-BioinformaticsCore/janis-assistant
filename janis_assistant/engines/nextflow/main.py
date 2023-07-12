import json
import os
import glob
import time
import re
import subprocess
import socketserver
import socket
import random
from typing import Dict, Any, Optional
from datetime import datetime
from http.server import BaseHTTPRequestHandler

from janis_core import LogLevel
from janis_core.types.data_types import is_python_primitive
from janis_core.utils.logger import Logger
from janis_core.translations import NextflowTranslator
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.run import RunModel
from janis_assistant.data.models.workflowjob import RunJobModel
from janis_assistant.engines.nextflow.nextflowconfiguration import NextflowConfiguration
from janis_assistant.engines.engine import Engine, TaskStatus
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.utils import ProcessLogger
from janis_assistant.utils.dateutils import DateUtil
from janis_assistant.management.configuration import JanisConfigurationNextflow
from janis_assistant.data.models.preparedjob import PreparedJob


def make_request_handler(nextflow_logger):
    class NextflowRequestHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len)
            body_as_str = post_body.decode("utf-8")
            body_as_json = json.loads(body_as_str)

            # Logger.debug(body_as_json)

            event = body_as_json.get("event")

            if event == "completed" or event == "error":
                Logger.debug("shutting down server")
                self.server.shutdown()
                Logger.debug("server shut down")

                self.read_cached_process(body_as_json)

                nextflow_logger.exit_function(nextflow_logger)
                nextflow_logger.terminate()
            elif event.startswith("process_"):
                trace = body_as_json.get("trace", {})
                name = trace.get("name")
                process = trace.get("process")
                task_id = trace.get("task_id")
                work_dir = trace.get("workdir")
                exit_code = trace.get("exit")
                process_id = trace.get("native_id")
                container = trace.get("container")
                err_message = self.read_error_message(work_dir)

                janis_status = self.read_process_status(body_as_json)
                start, finish = self.set_start_finish_time(janis_status)

                if process == NextflowTranslator.FINAL_STEP_NAME:
                    nextflow_logger.nf_monitor = NextFlowTaskMonitor(id=task_id, name=name, status=janis_status,
                                                                     exit=exit_code, work_dir=work_dir)
                else:
                    job = RunJobModel(
                        submission_id=None,
                        run_id=nextflow_logger.sid,
                        id_=name,
                        parent=None,
                        name=name,
                        status=janis_status,
                        start=start,
                        finish=finish,
                        workdir=work_dir,
                        stderr=os.path.join(work_dir, ".command.err"),
                        stdout=os.path.join(work_dir, ".command.out"),
                        script=os.path.join(work_dir, ".command.run"),
                        batchid=process_id,
                        container=container,
                        returncode=exit_code,
                        error=err_message
                    )

                    nextflow_logger.metadata_callback(nextflow_logger, job)
            elif event == "started":
                pass
            else:
                raise Exception(f"Unknown weblog request event {event}")

        @classmethod
        def read_error_message(cls, work_dir: str) -> Optional[str]:
            err_message = None

            file_path = os.path.join(work_dir, ".err")
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    err_message = f.read()

            return err_message

        def read_cached_process(self, data: dict):
            processes = data.get("metadata", {}).get("workflow", {}).get("workflowStats", {}).get("processes", [])

            for p in processes:
                if p["cached"] == 1:
                    name = p["name"]
                    janis_status = self.read_cached_process_status(p)
                    start, finish = self.set_start_finish_time(janis_status)

                    job = RunJobModel(
                        submission_id=None,
                        run_id=nextflow_logger.sid,
                        id_=name,
                        parent=None,
                        name=name,
                        status=janis_status,
                        start=start,
                        finish=finish,
                        workdir=None,
                        cached=True
                    )

                    nextflow_logger.metadata_callback(nextflow_logger, job)

        @classmethod
        def read_cached_process_status(cls, process_json: dict):
            if process_json.get("errored") == False:
                return TaskStatus.COMPLETED
            else:
                return TaskStatus.FAILED

        def read_process_status(self, data: dict):
            status = data["trace"]["status"]

            janis_status = TaskStatus.QUEUED
            if status == "COMPLETED":
                janis_status = TaskStatus.COMPLETED
            elif status == "RUNNING":
                janis_status = TaskStatus.RUNNING
            elif status == "SUBMITTED":
                janis_status = TaskStatus.QUEUED
            elif status == "FAILED":
                janis_status = TaskStatus.FAILED

            return janis_status

        def set_start_finish_time(self, janis_status: TaskStatus):
            start = DateUtil.now() if janis_status == TaskStatus.RUNNING else None
            finish = DateUtil.now() if janis_status == TaskStatus.COMPLETED else None

            return start, finish

    return NextflowRequestHandler


class NextflowLogger(ProcessLogger):

    def __init__(self, sid: str, process, nextflow_log_filename, logfp, metadata_callback, execution_directory,
                 listener_host, listener_port, listener_socket, exit_function=None):
        self.sid = sid

        self.error = None
        self.metadata_callback = metadata_callback
        self.outputs = None
        self.workflow_scope = []
        self.execution_directory = execution_directory
        self.work_directory = None
        self.nextflow_log_file = os.path.join(self.execution_directory, nextflow_log_filename)
        self.nf_monitor = None
        self.executor = None
        self.current_nf_monitor = None
        self.current_nf_submitter = None
        self.listener_host = listener_host
        self.listener_port = listener_port
        self.listener_socket = listener_socket
        super().__init__(
            process=process, prefix="nextflow: ", logfp=logfp, exit_function=exit_function
        )

    def run(self):
        try:
            httpd = None
            self.listener_socket.close()
            socketserver.ThreadingTCPServer.allow_reuse_address = True
            httpd = socketserver.ThreadingTCPServer((self.listener_host, self.listener_port),
                                                    make_request_handler(self))
            print("serving at port", self.listener_port)

            if httpd is not None:
                httpd.serve_forever()
            else:
                raise Exception("Failed to start http server to listen to Nextflow status update")

        except KeyboardInterrupt:
            self.should_terminate = True
            print("Detected keyboard interrupt")
            self.shutdown_listener(httpd)

        except Exception as e:
            print("Detected another error")
            if not self.shutdown_listener(httpd):
                self.exit_function(self)
                self.process.kill()
                self.terminate()
            raise

    def shutdown_listener(self, httpd):
        if httpd is not None:
            httpd.shutdown()

            return True

        return False


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
            configuration_dir: str,
            logfile=None,
            identifier: str = "nextflow",
            config: NextflowConfiguration = None,
    ):
        super().__init__(
            identifier, EngineType.nextflow, logfile=logfile, execution_dir=execution_dir
        )
        self.configuration_dir = configuration_dir

        self.process = None
        self._logger = None
        self.nextflow_log_filename = f"nextflow-{int(time.time())}.log"
        self.listener_host = "localhost"
        self.listener_port = None
        self.listener_socket = None

        self.taskmeta = {}

        self.find_or_generate_config(config)

    def find_or_generate_config(self, config: NextflowConfiguration):
        """
        Nextflow run configuration

        :param config:
        :type config:
        :param job:
        :type job:
        :return:
        :rtype:
        """
        job = PreparedJob.instance()

        if config:
            self.config = config
        else:
            self.config = (
                job.template.template.engine_config(EngineType.nextflow, job)
                or NextflowConfiguration(job)
            )

    def start_engine(self):
        return self

    def stop_engine(self):
        return self

    def terminate_task(self, identifier) -> TaskStatus:
        self.stop_engine()
        self.taskmeta["status"] = TaskStatus.ABORTED
        return TaskStatus.ABORTED

    def find_available_port(self):
        a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        range_min = 8000
        range_max = 8100
        available_port = None
        num_tries = 10
        while True and num_tries > 0:
            port = random.randint(range_min, range_max)
            num_tries -= 1

            location = (self.listener_host, port)
            result_of_check = a_socket.connect_ex(location)

            if result_of_check == 0:
                # Port is open
                continue
            else:
                # Port is not open
                available_port = port
                break

        if available_port is None:
            raise Exception(f"Cannot find available port between {range_min} and {range_max}")

        return port, a_socket

    def start_from_paths(self, wid, source_path: str, input_path: str, deps_path: str):
        Logger.debug(f"source_path: {source_path}")
        Logger.debug(f"input_path: {input_path}")
        Logger.debug(f"deps_path: {deps_path}")

        self.taskmeta = {
            "start": DateUtil.now(),
            "status": TaskStatus.PROCESSING,
            "jobs": {},
        }

        self.listener_port, self.listener_socket = self.find_available_port()
        Logger.info(f"Port {self.listener_port} is available")
        cmd = self.config.build_command_line(source_path=source_path, input_path=input_path,
                                             nextflow_log_filename=self.nextflow_log_filename,
                                             configuration_dir=self.configuration_dir,
                                             host=self.listener_host, port=self.listener_port)

        Logger.info(f"Running command: {cmd}")

        self.process = subprocess.Popen(
            cmd, preexec_fn=os.setsid,
        )

        Logger.info("Nextflow has started with pid=" + str(self.process.pid))
        self.process_id = self.process.pid

        self._logger = NextflowLogger(
            wid,
            self.process,
            nextflow_log_filename=self.nextflow_log_filename,
            logfp=open(self.logfile, "a+"),
            metadata_callback=self.task_did_update,
            execution_directory=self.execution_dir,
            listener_host=self.listener_host,
            listener_port=self.listener_port,
            listener_socket=self.listener_socket,
            exit_function=self.task_did_exit
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
