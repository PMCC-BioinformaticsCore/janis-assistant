import os
import tempfile
from typing import Optional

import json
import requests
import signal
import subprocess
import time

from janis_runner.data.models.schema import TaskMetadata
from janis_runner.utils import ProcessLogger, write_files_into_buffered_zip
from janis_runner.utils.dateutil import DateUtil
from janis_runner.utils.logger import Logger
from janis_runner.engines.cromwell.metadata import (
    cromwell_status_to_status,
    CromwellMetadata,
)

from .data_types import CromwellFile
from .configurations import CromwellConfiguration
from ..engine import Engine, TaskStatus, TaskBase


class Cromwell(Engine):

    environment_map = {"default": "localhost:8000"}

    def db_to_kwargs(self, keys: [str] = None):
        return super(Cromwell, self).db_to_kwargs(
            ["host", "cromwell_loc", "config_path"]
        )

    def __init__(
        self, identifier="cromwell", cromwell_loc=None, config_path=None, config=None, host=None
    ):

        super().__init__(identifier, Engine.EngineType.cromwell)

        self.cromwell_loc = cromwell_loc

        if config and not config_path:
            f = tempfile.NamedTemporaryFile(mode="w+t", suffix=".conf", delete=False)
            lines = config
            if isinstance(config, CromwellConfiguration):
                lines = config.output()
            f.writelines(lines)
            f.seek(0)
            config_path = f.name

        self.connect_to_instance = True if host else False
        self.host = host if host else "localhost:8000"

        self.config_path = config_path
        self.process = None
        self.logger = None
        self.stdout = []

    @staticmethod
    def from_url(identifier, url):
        if not url:
            raise Exception(
                "No url was provided to 'Cromwell.from_url', this field is required for expected results"
            )
        return Cromwell(identifier=identifier, host=url)

    def start_engine(self):

        if self.connect_to_instance:
            return Logger.info(
                "Cromwell environment discovered, skipping local instance"
            )

        if self.process:
            return Logger.info(
                f"Discovered Cromwell instance (pid={self.process}), skipping start"
            )

        Logger.log("Finding cromwell jar")
        cromwell_loc = self.cromwell_loc if self.cromwell_loc else os.getenv("cromwelljar")

        if not cromwell_loc:
            raise Exception(
                'Couldn\'t get $cromwelljar from the environment, `export cromwell="path/to/cromwell.jar"`'
            )

        Logger.info("Starting cromwell ...")
        cmd = ["java", "-jar"]
        if self.config_path:
            Logger.log("Using configuration file for Cromwell: " + self.config_path)
            cmd.append("-Dconfig.file=" + self.config_path)
        cmd.extend([cromwell_loc, "server"])

        Logger.log(f"Starting Cromwell with command: '{' '.join(cmd)}'")
        self.process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid
        )
        Logger.log("Cromwell is starting with pid=" + str(self.process.pid))
        Logger.log(
            "Cromwell will start the HTTP server, reading logs to determine when this occurs"
        )
        for c in iter(
            self.process.stdout.readline, "b"
        ):  # replace '' with b'' for Python 3
            cd = c.decode("utf-8").rstrip()

            if not cd:
                continue
            Logger.log("Cromwell: " + cd)
            # self.stdout.append(str(c))
            if "service started on" in cd:
                Logger.info(
                    "Service successfully started with pid=" + str(self.process.pid)
                )
                break

        Logger.log("Waiting 1 second for Cromwell to get ready")
        time.sleep(3)
        Logger.log("Proceeding after intention Cromwell delay")

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

    def url_poll(self, identifier):
        return self.url_base() + f"/{identifier}/status"

    def url_outputs(self, identifier):
        return self.url_base() + f"/{identifier}/outputs"

    def url_metadata(self, identifier, expand_subworkflows=True):
        return (
            self.url_base()
            + f"/{identifier}/metadata?expandSubWorkflows={expand_subworkflows}"
        )

    def url_abort(self, identifier):
        return self.url_base() + f"/{identifier}/abort"

    def create_task(self, tid, source, inputs: list, dependencies, workflow_type=None):
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
            raise Exception(
                "Too many inputs (yaml files). Proposed: automatic merge into one file."
            )

        files = {
            "workflowSource": source,
            "labels": json.dumps({"taskid": tid}),
            "workflowOptions": json.dumps(
                {
                    "google_labels": {"taskid": tid},
                    "monitoring_image": "quay.io/dinvlad/cromwell-monitor",
                    "workflow_failure_mode": "ContinueWhilePossible"
                }
            ),
        }

        if dependencies:
            files["workflowDependencies"] = dependencies

        for i in range(len(inputs)):
            k = "workflowInputs" + ("" if i == 0 else "_" + str(i + 1))
            files[k] = inputs[i]

        Logger.log("Posting to " + url)
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
        url = self.url_poll(identifier=identifier)
        r = requests.get(url)
        res = r.json()
        return cromwell_status_to_status(res["status"])

    def outputs_task(self, identifier):
        url = self.url_outputs(identifier=identifier)
        r = requests.get(url)
        if not r.ok:
            return Logger.warn(
                f"Couldn't get outputs with identifier='${identifier}', got status: "
                + str(r.status_code)
            )
        try:
            res = r.json()
            outs = res.get("outputs")
        except Exception as e:
            return Logger.warn(
                f"Couldn't get outputs with identifier='${identifier}', got error: "
                + str(e)
            )

        if not outs:
            return {}
        return {
            k: CromwellFile.parse(outs[k]) if isinstance(outs[k], dict) else outs[k]
            for k in outs
        }

    def start_from_paths(
        self, tid: str, source_path: str, input_path: str, deps_path: str
    ):
        """
        This does NOT watch, it purely schedule the jobs
        :param tid:
        :param source_path:
        :param input_path:
        :param deps_path:
        :return:
        """
        src = open(source_path, "rb")
        inp = open(input_path, "rb")
        deps = open(deps_path, "rb")
        engid = self.create_task(tid, src, [inp], deps, workflow_type="wdl")

        src.close()
        inp.close()
        deps.close()
        return engid

    def start_from_task(self, task: TaskBase):
        """
        This watches the job, and calls a task handler / onerror if it can
        :param task:
        :return:
        """
        Logger.log("Creating workflow and submitting to Cromwell")

        ins, deps = [], None
        if task.inputs or task.input_paths:
            for t in task.inputs if task.inputs else task.input_paths:
                if isinstance(t, dict):
                    import ruamel.yaml

                    ins.append(ruamel.yaml.dump(t, default_flow_style=False))
                elif task.inputs:
                    ins.append(t)
                else:
                    ins = open(t, "rb")

        if task.dependencies:
            deps = write_files_into_buffered_zip(task.dependencies)
        elif task.dependencies_path:
            deps = (
                task.dependencies
                if task.dependencies
                else open(task.dependencies_path, "rb")
            )

        task.identifier = self.create_task(
            tid=task.tid,
            source=task.source if task.source else open(task.source_path, "rb"),
            inputs=ins,
            dependencies=deps,
        )
        Logger.info("Created task with id: " + task.identifier)
        Logger.log("Task is now processing")
        task.task_start = DateUtil.now()

        while task.status not in TaskStatus.final_states():
            status = self.poll_task(task.identifier)
            if status != task.status:
                Logger.info(
                    "Task ('{id}') has progressed to: '{status}'".format(
                        id=task.identifier, status=status
                    )
                )
            task.status = status

            if task.status not in TaskStatus.final_states():
                time.sleep(1)

        task.task_finish = DateUtil.now()
        Logger.info(
            "Task ('{id}') has finished processing: {t} seconds".format(
                id=task.identifier,
                t=str((task.task_finish - task.task_start).total_seconds()),
            )
        )

        if task.status == TaskStatus.COMPLETED:
            Logger.log("Collecting outputs")
            task.outputs = self.outputs_task(task.identifier)

    def raw_metadata(
        self, identifier, expand_subworkflows=True
    ) -> Optional[CromwellMetadata]:
        url = self.url_metadata(
            identifier=identifier, expand_subworkflows=expand_subworkflows
        )
        Logger.log(f"Getting Cromwell metadata for task '{identifier}' with url: {url}")
        r = requests.get(url)
        try:
            r.raise_for_status()
            return CromwellMetadata(r.json())

        except Exception as e:
            print(e)
            return None

    def metadata(self, identifier, expand_subworkflows=True) -> Optional[TaskMetadata]:
        raw = self.raw_metadata(identifier, expand_subworkflows=expand_subworkflows)
        return raw.standard() if raw else raw

    def terminate_task(self, identifier) -> TaskStatus:
        url = self.url_abort(identifier)
        r = requests.post(url)
        Logger.log("Cromwell (Abort): " + str(r))
        return TaskStatus.TERMINATED
