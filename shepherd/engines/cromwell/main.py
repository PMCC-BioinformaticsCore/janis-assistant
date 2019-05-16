import os
import tempfile
import requests
import signal
import subprocess
import time
from datetime import datetime


from shepherd.engines.cromwell.data_types import CromwellFile
from shepherd.engines.cromwell.configurations import CromwellConfiguration
from shepherd.engines.engine import Engine, TaskStatus, TaskBase
from shepherd.utils import ProcessLogger, write_files_into_buffered_zip
from shepherd.utils.logger import Logger
from shepherd.engines.cromwell.metadata import cromwell_status_to_status, CromwellMetadata


class Cromwell(Engine):

    environment_map = {
        "default": "localhost:8000",
        "pmac": "vmdv-res-seq.unix.petermac.org.au:8000"
    }

    def __init__(self, cromwell_loc=None, config_path=None, config=None, environment=None):
        self.cromwell_loc = cromwell_loc

        if config and not config_path:
            f = tempfile.NamedTemporaryFile(mode="w+t", suffix=".conf", delete=False)
            lines = config
            if isinstance(config, CromwellConfiguration):
                lines = config.output()
            f.writelines(lines)
            f.seek(0)
            config_path = f.name

        if environment and environment not in Cromwell.environment_map:
            raise Exception(f"The environment '{environment}' was not recognised")

        self.environment = environment
        host = Cromwell.environment_map.get(environment)
        self.host = host if host else Cromwell.environment_map.get("default")

        self.config_path = config_path
        self.process = None
        self.logger = None
        self.stdout = []

    def start_engine(self):

        if self.environment:
            return Logger.info("Cromwell environment discovered, skipping local instance")

        if self.process:
            return Logger.info(f"Discovered Cromwell instance (pid={self.process}), skipping start")

        Logger.info("Starting cromwell ...")

        cromwell_loc = self.cromwell_loc if self.cromwell_loc else os.getenv("cromwell")

        if not cromwell_loc:
            raise Exception("Couldn't get $cromwell from the environment, `export cromwell=\"path/to/cromwell.jar\"`")

        cmd = ["java", "-jar"]
        if self.config_path:
            Logger.log("Using configuration file for Cromwell: " + self.config_path)
            cmd.append("-Dconfig.file=" + self.config_path)
        cmd.extend([cromwell_loc, "server"])

        Logger.log(f"Starting Cromwell with command: '{' '.join(cmd)}'")
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid)
        Logger.log("Cromwell is starting with pid=" + str(self.process.pid))
        Logger.log("Cromwell will start the HTTP server, reading logs to determine when this occurs")
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            cd = c.decode("utf-8").rstrip()

            if not cd: continue
            Logger.log("Cromwell: " + cd)
            # self.stdout.append(str(c))
            if "service started on" in cd:
                Logger.info("Service successfully started with pid=" + str(self.process.pid))
                break

        if self.process:
            self.logger = ProcessLogger(self.process, "Cromwell: ")
        return self

    def stop_engine(self):
        self.logger.terminate()
        if not self.process:
            Logger.warn("Could not find a cromwell process to end, SKIPPING")
            return
        Logger.info("Stopping cromwell")
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        Logger.log("Stopped cromwell")

    # API

    version = "v1"

    def url_base(self):
        return f"http://{self.host}/api/workflows/" + str(Cromwell.version)

    def url_create(self):
        return self.url_base()

    def url_poll(self, id):
        return self.url_base() + f"/{id}/status"

    def url_outputs(self, id):
        return self.url_base() + f"/{id}/outputs"

    def url_metadata(self, id, expand_workflows=True):
        return self.url_base() + f"/{id}/metadata?expandSubWorkflows={expand_workflows}"

    def create_task(self, source, inputs: list, dependencies, workflow_type="cwl"):
        # curl \
        #   -X POST "http://localhost:8000/api/workflows/v1" \
        #   -H "accept: application/json" \
        #   -H "Content-Type: multipart/form-data" \
        #   -F "workflowSource=@whole_genome_germline.cwl;type=" \
        #   -F "workflowInputs=@whole_genome_germline-local.yml;type=" \
        #   -F "workflowDependencies=@tools-gatk4.0.1.2.zip;type=application/zip

        url = self.url_create()

        max_inputs = 5
        if len(inputs) > max_inputs:
            raise Exception("Too many inputs (yaml files). Proposed: automatic merge into one file.")

        files = {
            "workflowSource": source,
        }

        if dependencies:
            files["workflowDependencies"] = dependencies

        for i in range(len(inputs)):
            k = "workflowInputs" + ("" if i == 0 else "_" + str(i+1))
            files[k] = inputs[i]

        r = requests.post(url, files=files)
        try:
            res = r.json()
        except Exception as e:
            Logger.log_ex(e)
            Logger.critical(r.text)
            raise e

        if not r.ok or r.status_code > 201 or res["status"] != "Submitted":
            raise Exception(res)

        task_id = res["id"]
        return task_id

    def poll_task(self, identifier) -> TaskStatus:
        url = self.url_poll(id=identifier)
        r = requests.get(url)
        res = r.json()
        return cromwell_status_to_status(res["status"])

    def outputs_task(self, identifier):
        url = self.url_outputs(id=identifier)
        r = requests.get(url)
        res = r.json()
        outs = res.get("outputs")
        if not outs: return {}
        return {k: CromwellFile.parse(outs[k]) if isinstance(outs[k], dict) else outs[k] for k in outs}

    def start_task(self, task: TaskBase):
        Logger.log("Creating workflow and submitting to Cromwell")

        ins, deps = [], None
        if task.inputs or task.input_paths:
            for t in (task.inputs if task.inputs else task.input_paths):
                if isinstance(t, dict):
                    import ruamel.yaml
                    ins.append(ruamel.yaml.dump(t, default_flow_style=False))
                elif task.inputs:
                    ins.append(t)
                else:
                    ins = open(t, 'rb')

        if task.dependencies:
            deps = write_files_into_buffered_zip(task.dependencies)
        elif task.dependencies_path:
            deps = task.dependencies if task.dependencies else open(task.dependencies_path, 'rb')

        task.identifier = self.create_task(
            source=task.source if task.source else open(task.source_path, 'rb'),
            inputs=ins,
            dependencies=deps
        )
        Logger.info("Created task with id: " + task.identifier)
        Logger.log("Task is now processing")
        task.task_start = datetime.now()
        while task.status not in TaskStatus.FINAL_STATES():
            status = self.poll_task(task.identifier)
            if status != task.status:
                Logger.info("Task ('{id}') has progressed to: '{status}'".format(id=task.identifier, status=status))
            task.status = status
            time.sleep(1)

        task.task_finish = datetime.now()
        Logger.info("Task ('{id}') has finished processing: {t} seconds"
                    .format(id=task.identifier, t=str((task.task_finish - task.task_start).total_seconds())))

        if task.status == TaskStatus.COMPLETED:
            Logger.log("Collecting outputs")
            task.outputs = self.outputs_task(task.identifier)

    def metadata(self, identifier, expand_subworkflows=False):
        """
        calls: {
            backend
            backendLogs
            backendStatus
            end
            executionStatus
            failures
            inputs
            jobId
            returnCode
            start
            stderr
            stdout
        }
        failures: [{failure, timestamp}]
        id:
        inputs:
        outputs:
        start:
        status:
        submission:
        :param identifier:
        :param expand_subworkflows:
        :return:
        """



