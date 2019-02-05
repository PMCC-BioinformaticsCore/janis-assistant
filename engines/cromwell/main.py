import os
import signal
import requests
import threading
import subprocess

from engines.engine import Engine, TaskStatus
from utils.logger import Logger


class ProcessLogger(threading.Thread):
    def __init__(self, process, prefix):
        threading.Thread.__init__(self)
        self.should_terminate = False
        self.process = process
        self.prefix = prefix

    def terminate(self):
        self.should_terminate = True

    def run(self):
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if self.should_terminate: return
            if not c: continue
            Logger.log(self.prefix + str(c))


class Cromwell(Engine):

    def __init__(self, cromwell_loc="/Users/franklinmichael/broad/cromwell-36.jar"):
        self.cromwell_loc = cromwell_loc

        self.process = None
        self.logger = None
        self.stdout = []

    def start_engine(self):
        Logger.info("Starting cromwell ...")
        cmd = ["java", "-jar", self.cromwell_loc, "server"]
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid)
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if not c: continue
            Logger.log("Cromwell: " + str(c))
            # self.stdout.append(str(c))
            if "service started on" in str(c):
                Logger.info("Service successfully started")
                break

        if self.process:
            self.logger = ProcessLogger(self.process, "Cromwell: ")
            self.logger.start()

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
    url_base = f"http://localhost:8000/api/workflows/{version}"
    url_create = url_base
    url_poll = url_base + "/{id}/status"
    url_outputs = url_base + "/{id}/outputs"

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
            "workflowSource": open(source, "rb"),
        }

        if dependencies:
            files["workflowDependencies"] = open(dependencies, "rb")

        for i in range(len(inputs)):
            k = "workflowInputs" + ("" if i == 0 else f"_{i+1}")
            files[k] = open(inputs[i], "rb")

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

    def poll_task(self, identifier):
        url = Cromwell.url_poll.format(id=identifier)
        r = requests.get(url)
        res = r.json()
        return self._cromwell_status_to_status(res["status"])

    def outputs_task(self, identifier):
        url = Cromwell.url_outputs.format(id=identifier)
        r = requests.get(url)
        res = r.json()
        return res["outputs"]

    @staticmethod
    def _cromwell_status_to_status(status) -> TaskStatus:
        if status == "fail":
            return TaskStatus.QUEUED
        elif status == "Submitted":
            return TaskStatus.QUEUED
        elif status == "Running":
            return TaskStatus.RUNNING
        elif status == "Succeeded":
            return TaskStatus.COMPLETED
        else:
            return TaskStatus.FAILED
