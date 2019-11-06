import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from glob import glob
from typing import Optional, List

import requests
from janis_core.utils.logger import Logger

from janis_runner.__meta__ import ISSUE_URL
from janis_runner.data.models.outputs import WorkflowOutputModel
from janis_runner.data.models.workflow import WorkflowModel
from janis_runner.engines.cromwell.cromwellmetadata import (
    cromwell_status_to_status,
    CromwellMetadata,
)
from janis_runner.engines.enginetypes import EngineType
from janis_runner.utils import (
    ProcessLogger,
    write_files_into_buffered_zip,
    find_free_port,
)
from janis_runner.utils.dateutil import DateUtil
from .cromwellconfiguration import CromwellConfiguration
from ..engine import Engine, TaskStatus

CROMWELL_RELEASES = (
    "https://api.github.com/repos/broadinstitute/cromwell/releases/latest"
)

ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")


class Cromwell(Engine):
    def description(self):
        pid = ""
        if self.process_id:
            pid = f" [PID={self.process_id}]"
        return f"cromwell ({self.host}){pid}"

    def __setstate__(self, state):
        super().__setstate__(state)
        self._logger = None
        self._process = None

    def __init__(
        self,
        logfile=None,
        confdir=None,
        identifier="cromwell",
        host=None,
        cromwelljar=None,
        config: CromwellConfiguration = None,
        config_path=None,
        watch=True,
    ):

        super().__init__(identifier, EngineType.cromwell, watch=watch, logfile=logfile)

        # Heirarchy of configs:
        #   - Passed in config
        #   - Passed in configPath
        #   - Config available from JanisConfiguration
        #   - ConfigPath available from JanisConfiguration

        self.cromwelljar = cromwelljar
        self.connect_to_instance = True if host else False
        self.is_started = self.connect_to_instance

        self.host = host
        self.port = None
        self.config_path = None
        self._process = None
        self._logger = None
        self.stdout = []
        self.error_message = None

        if not self.connect_to_instance:

            # To avoid conflicts between version of Cromwell, we'll find an open
            # port, and allow Cromwell to bind there.
            self.port = find_free_port()
            self.host = f"localhost:{self.port}"

            self.config = None
            self.config_path = os.path.join(confdir, "cromwell.conf")
            self.find_or_generate_config(
                identifier, config=config, config_path=config_path
            )

    @staticmethod
    def from_url(identifier, url):
        if not url:
            raise Exception(
                "No url was provided to 'Cromwell.from_url', this field is required for expected results"
            )
        return Cromwell(identifier=identifier, host=url)

    def test_connection(self):
        if not self.is_started:
            return False

        try:
            r = requests.get(self.url_test())
            r.raise_for_status()
            return True

        except Exception as e:
            Logger.warn("Couldn't connect to Cromwell ({self.host}): " + str(e))
            return False

    def start_engine(self, additional_cromwell_options: List[str] = None):

        if self.test_connection():
            Logger.info("Engine has already been started")
            return self

        if self.connect_to_instance:
            self.is_started = True
            Logger.info("Cromwell environment discovered, skipping local instance")
            return self

        if self._process:
            self.is_started = True
            Logger.info(
                f"Discovered Cromwell instance (pid={self._process}), skipping start"
            )
            return self

        if self.config:
            with open(self.config_path, "w+") as f:
                f.writelines(self.config.output())

        Logger.log("Finding cromwell jar")
        cromwell_loc = self.resolve_jar(self.cromwelljar)

        Logger.info(f"Starting cromwell ({os.path.basename(cromwell_loc)})...")
        cmd = ["java", "-DLOG_MODE=pretty"]

        if additional_cromwell_options:
            cmd.extend(additional_cromwell_options)

        if self.port:
            cmd.append(f"-Dwebservice.port={self.port}")
        cmd.append(f"-Dwebservice.interface=127.0.0.1")

        if self.config_path and os.path.exists(self.config_path):
            Logger.log("Using configuration file for Cromwell: " + self.config_path)
            cmd.append("-Dconfig.file=" + self.config_path)
        cmd.extend(["-jar", cromwell_loc, "server"])

        Logger.log(f"Starting Cromwell with command: '{' '.join(cmd)}'")
        self._process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid
        )
        Logger.log("Cromwell is starting with pid=" + str(self._process.pid))
        Logger.log(
            "Cromwell will start the HTTP server, reading logs to determine when this occurs"
        )
        for c in iter(
            self._process.stdout.readline, "b"
        ):  # replace '' with b'' for Python 3
            cd = c.decode("utf-8").rstrip()

            if not cd:
                continue

            Logger.log("Cromwell: " + cd)

            # self.stdout.append(str(c))
            if "service started on" in cd:
                self.process_id = self._process.pid
                Logger.info(
                    "Service successfully started with pid=" + str(self._process.pid)
                )
                break
            # elif ansi_escape.match():
            #     raise Exception(cd)

        self.is_started = True

        if self._process:
            self._logfp = open(self.logfile, "a+")
            Logger.info(
                "Will log to file" if bool(self._logfp) else "Will NOT log to file"
            )
            self._logger = ProcessLogger(self._process, "Cromwell: ", self._logfp)
        return self

    def did_fail(self, rc):
        if rc == 0:
            return

        self.error_message = (
            "Cromwell exited with rc = {rc}, check the engine log for more information"
        )
        self.process_id = None
        self.process = None

    def stop_engine(self):
        if self._logger:
            self._logger.terminate()

        if self._logfp:
            self._logfp.flush()
            os.fsync(self._logfp.fileno())
            self._logfp.close()

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

    def url_test(self):
        return f"http://{self.host}/engine/v1/version"

    def url_metadata(self, identifier, expand_subworkflows=True):
        return (
            self.url_base()
            + f"/{identifier}/metadata?expandSubWorkflows={expand_subworkflows}"
        )

    def url_abort(self, identifier):
        return self.url_base() + f"/{identifier}/abort"

    def create_task(self, wid, source, inputs: list, dependencies, workflow_type=None):
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
            "labels": json.dumps({"taskid": wid}),
            "workflowOptions": json.dumps(
                {
                    "google_labels": {"taskid": wid},
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
        from janis_runner.management.configuration import JanisConfiguration

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

            progress_is_loaded = False
            try:
                import progressbar

                progress_is_loaded = True
            except:
                Logger.critical("Couldn't find progressbar module")

            pbar = None

            def show_progress(block_num, block_size, total_size):
                nonlocal pbar
                if pbar is None and progress_is_loaded:
                    pbar = progressbar.ProgressBar(maxval=total_size)
                downloaded = block_num * block_size
                if downloaded < total_size:
                    if pbar:
                        pbar.update(downloaded)
                    else:
                        print(
                            f"\rProgress: {round(downloaded * 100 / total_size)}%",
                            end="",
                            file=sys.stderr,
                        )

                else:
                    if pbar:
                        pbar.finish()
                        pbar = None
                    else:
                        print("\rCompleted download of cromwell", file=sys.stderr)

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
        if self.error_message:
            return TaskStatus.FAILED

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
        parsed = [self.parse_output(k, v) for k, v in outs.items()]
        return {out[0]: out[1] for out in parsed}

    @staticmethod
    def parse_output(key, value):
        newkey = "".join(key.split(".")[1:])

        fileloc = value
        if isinstance(value, dict):
            fileloc = value["location"]

        if isinstance(fileloc, list):
            return newkey, [Cromwell.parse_output(key, f)[1] for f in fileloc]

        return (
            newkey,
            WorkflowOutputModel(
                tag=newkey,
                original_path=fileloc,
                timestamp=DateUtil.now(),
                new_path=None,
                tags=None,
                prefix=None,
                secondaries=None,
                extension=None,
            ),
        )

    def start_from_paths(
        self, wid: str, source_path: str, input_path: str, deps_path: str
    ):
        """
        This does NOT watch, it purely schedule the jobs
        :param wid:
        :param source_path:
        :param input_path:
        :param deps_path:
        :return:
        """
        src = open(source_path, "rb")
        inp = open(input_path, "rb")
        deps = open(deps_path, "rb")
        engid = self.create_task(wid, src, [inp], deps, workflow_type="wdl")

        src.close()
        inp.close()
        deps.close()
        return engid

    def find_or_generate_config(
        self, identifier, config: CromwellConfiguration, config_path
    ):
        from janis_runner.management.configuration import JanisConfiguration

        jc = JanisConfiguration.manager()

        if config:
            self.config = config

        elif config_path:
            shutil.copyfile(config_path, self.config_path)

        elif jc.cromwell.configpath:
            shutil.copyfile(jc.cromwell.configpath, self.config_path)

        else:
            self.config: CromwellConfiguration = jc.template.template.engine_config(
                EngineType.cromwell
            ) or CromwellConfiguration()
            if not self.config.system:
                self.config.system = CromwellConfiguration.System()
            self.config.system.cromwell_id = identifier
            self.config.system.cromwell_id_random_suffix = False
            self.config.system.job_shell = ("job-shell", "/bin/sh")

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

        except requests.HTTPError as e:
            if e.response.status_code == 404:
                # Usually means Cromwell hasn't loaded properly yet
                return None

            try:
                res = e.response.json()
                message = res["message"]
                Logger.warn("Response when getting Cromwell metadata: " + str(message))
            except Exception as ee:
                Logger.warn(str(e))
            finally:
                return None

    def metadata(self, identifier, expand_subworkflows=True) -> Optional[WorkflowModel]:
        if self.error_message:
            return WorkflowModel(error=self.error_message, status=TaskStatus.FAILED)

        raw = self.raw_metadata(identifier, expand_subworkflows=expand_subworkflows)
        return raw.standard() if raw else raw

    def terminate_task(self, identifier) -> TaskStatus:
        url = self.url_abort(identifier)
        r = requests.post(url)
        Logger.log("Cromwell (Abort): " + str(r))
        return TaskStatus.ABORTED
