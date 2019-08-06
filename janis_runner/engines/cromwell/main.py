import os
import tempfile
import progressbar
import urllib.request
from glob import glob
from typing import Optional

import json
import requests
import signal
import subprocess
import time

from janis_runner.__meta__ import ISSUE_URL
from janis_runner.data.models.schema import TaskMetadata
from janis_runner.management.configuration import JanisConfiguration
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


CROMWELL_RELEASES = (
    "https://api.github.com/repos/broadinstitute/cromwell/releases/latest"
)


class Cromwell(Engine):
    def db_to_kwargs(self, keys: [str] = None):
        return super(Cromwell, self).db_to_kwargs(
            [*(keys or []), "host", "process_id", "config_path"]
        )

    def __init__(
        self,
        identifier="cromwell",
        host=None,
        cromwelljar=None,
        config_path=None,
        config=None,
    ):

        super().__init__(identifier, Engine.EngineType.cromwell)

        if config and not config_path:
            f = tempfile.NamedTemporaryFile(mode="w+t", suffix=".conf", delete=False)
            lines = config
            if isinstance(config, CromwellConfiguration):
                lines = config.output()
            f.writelines(lines)
            f.seek(0)
            config_path = f.name

        self.cromwelljar = cromwelljar
        self.connect_to_instance = True if host else False
        self.is_started = self.connect_to_instance
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

        if self.is_started:
            return Logger.info("Engine has already been started")

        if self.connect_to_instance:
            self.is_started = True
            return Logger.info(
                "Cromwell environment discovered, skipping local instance"
            )

        if self.process:
            self.is_started = True
            return Logger.info(
                f"Discovered Cromwell instance (pid={self.process}), skipping start"
            )

        Logger.log("Finding cromwell jar")
        cromwell_loc = self.resolve_jar(self.cromwelljar)

        Logger.info("Starting cromwell ...")
        cmd = ["java", "-DLOG_MODE=pretty"]
        if self.config_path:
            Logger.log("Using configuration file for Cromwell: " + self.config_path)
            cmd.append("-Dconfig.file=" + self.config_path)
        cmd.extend(["-jar", cromwell_loc, "server"])

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
                self.process_id = self.process.pid
                Logger.info(
                    "Service successfully started with pid=" + str(self.process.pid)
                )
                break

        self.is_started = True

        if self.process:
            self.logger = ProcessLogger(self.process, "Cromwell: ")
        return self

    def stop_engine(self):
        if self.logger:
            self.logger.terminate()
        if not self.process_id:
            Logger.warn("Could not find a cromwell process to end, SKIPPING")
            return
        Logger.info("Stopping cromwell")
        process = os.getpgid(int(self.process_id))
        if process:
            os.killpg(process, signal.SIGTERM)
        Logger.log("Stopped cromwell")
        self.is_started = False

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
                    "workflow_failure_mode": "ContinueWhilePossible",
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

    def resolve_jar(self, cromwelljar):
        man = JanisConfiguration.manager()
        if not man:
            raise Exception(
                f"No configuration was initialised. This is "
                f"likely an error, and you should raise an issue at {ISSUE_URL}"
            )

        potentials = []

        if cromwelljar:
            potentials.append(os.path.expanduser(cromwelljar))
        if man.cromwell.jarpath:
            potentials.append(os.path.expanduser(cromwelljar))
        potentials.extend(
            reversed(sorted(glob(os.path.join(man.configdir + "cromwell-*.jar"))))
        )

        valid_paths = [p for p in potentials if os.path.exists(p)]
        if len(potentials) > 0:
            if len(valid_paths) == 0:
                raise Exception(
                    "Couldn't find cromwelljar at any of the required paths: "
                    + ", ".join(potentials)
                )
            cromwelljar = valid_paths[0]

        if not cromwelljar:

            pbar = None

            def show_progress(block_num, block_size, total_size):
                nonlocal pbar
                if pbar is None:
                    pbar = progressbar.ProgressBar(maxval=total_size)
                downloaded = block_num * block_size
                if downloaded < total_size:
                    pbar.update(downloaded)
                else:
                    pbar.finish()
                    pbar = None

            cromwellurl, cromwellfilename = self.get_latest_cromwell_url()
            Logger.info(
                f"Couldn't find cromwell at any of the usual spots, downloading '{cromwellfilename}' now"
            )
            cromwelljar = os.path.join(man.configdir, cromwellfilename)
            urllib.request.urlretrieve(cromwellurl, cromwelljar, show_progress)
            Logger.info(f"Downloaded {cromwellfilename}")

        return cromwelljar

    @staticmethod
    def get_latest_cromwell_url():
        data = urllib.request.urlopen(CROMWELL_RELEASES).read()
        releases = json.loads(data)
        asset = releases.get("assets")[0]
        return asset.get("browser_download_url"), asset.get("name")

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
        try:
            r = requests.get(url)
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
