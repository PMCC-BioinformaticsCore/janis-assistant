import os, time
import signal
import requests
import threading
import subprocess

from datetime import datetime

from runner.engines.cromwell.data_types import CromwellFile
from runner.engines.engine import Engine, TaskStatus
from runner.utils import ProcessLogger
from runner.utils.logger import Logger

class Cromwell(Engine):

    def __init__(self, cromwell_loc="/Users/franklinmichael/broad/cromwell-36.jar", config_path=None):
        self.cromwell_loc = cromwell_loc

        self.config_path = config_path
        self.process = None
        self.logger = None
        self.stdout = []

    def start_engine(self):
        Logger.info("Starting cromwell ...")

        cmd = ["java", "-jar"]
        if self.config_path:
            Logger.log("Using configuration file for Cromwell: " + self.config_path)
            cmd.append("-Dconfig.file=" + self.config_path)
        cmd.extend([self.cromwell_loc, "server"])
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid)
        Logger.log("Cromwell is starting with pid=" + str(self.process.pid))
        Logger.log("Cromwell will start the HTTP server, reading logs to determine when this occurs")
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            cd = c.decode("utf-8").strip()
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
    url_base = "http://localhost:8000/api/workflows/" + str(version)
    url_create = url_base
    url_poll = url_base + "/{id}/status"
    url_outputs = url_base + "/{id}/outputs"
    url_metadata = url_base + "/{id}/metadata"

    def create_task(self, source, inputs: list, dependencies, workflow_type="cwl"):
        # curl \
        #   -X POST "http://localhost:8000/api/workflows/v1" \
        #   -H "accept: application/json" \
        #   -H "Content-Type: multipart/form-data" \
        #   -F "workflowSource=@whole_genome_germline.cwl;type=" \
        #   -F "workflowInputs=@whole_genome_germline-local.yml;type=" \
        #   -F "workflowDependencies=@tools-gatk4.0.1.2.zip;type=application/zip

        url = Cromwell.url_base

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
        url = Cromwell.url_poll.format(id=identifier)
        r = requests.get(url)
        res = r.json()
        return self._cromwell_status_to_status(res["status"])

    def outputs_task(self, identifier):
        url = Cromwell.url_outputs.format(id=identifier)
        r = requests.get(url)
        res = r.json()
        outs = res.get("outputs")
        if not outs: return {}
        return {k: CromwellFile.parse(outs[k]) if isinstance(outs[k], dict) else outs[k] for k in outs}

    def start_task(self, task):
        Logger.log("Creating workflow and submitting to Cromwell")

        ins, deps = [], None
        if task.inputs or task.input_paths:
            ins = task.inputs if task.inputs else [open(i) for i in task.input_paths]
        if task.dependencies or task.dependencies_path:
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


    @staticmethod
    def _cromwell_status_to_status(status) -> TaskStatus:
        if status == "fail":
            return TaskStatus.PROCESSING
        elif status == "Submitted":
            return TaskStatus.QUEUED
        elif status == "Running":
            return TaskStatus.RUNNING
        elif status == "Succeeded":
            return TaskStatus.COMPLETED
        else:
            return TaskStatus.FAILED
