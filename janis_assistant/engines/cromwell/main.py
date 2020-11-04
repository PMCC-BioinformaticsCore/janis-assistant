import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
from datetime import datetime
from glob import glob
from typing import Optional, List, Tuple, Union
from urllib import request, parse

from janis_core.utils import first_value
from janis_core.utils.logger import Logger

from janis_assistant.__meta__ import ISSUE_URL, GITHUB_URL
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.run import RunModel
from janis_assistant.engines.cromwell.cromwellmetadata import (
    cromwell_status_to_status,
    CromwellMetadata,
)
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.management.envvariables import EnvVariables
from janis_assistant.utils import (
    ProcessLogger,
    find_free_port,
)
from janis_assistant.utils.dateutil import DateUtil
from janis_assistant.utils.fileutil import tail
from .cromwellconfiguration import CromwellConfiguration, DatabaseTypeToUse
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
        self._timer_thread = None
        self._start_time = DateUtil.now()

    def __init__(
        self,
        logfile=None,
        confdir=None,
        identifier="cromwell",
        host=None,
        cromwelljar=None,
        config: CromwellConfiguration = None,
        config_path=None,
        execution_dir: str = None,
        polling_interval: Optional[int] = None,
        db_type: DatabaseTypeToUse = None,
    ):

        super().__init__(
            identifier,
            EngineType.cromwell,
            logfile=logfile,
            execution_dir=execution_dir,
        )

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
        self._timer_thread: Optional[threading.Event] = None
        self.config: Optional[CromwellConfiguration] = None
        # Last contacted is used to determine
        self.last_contacted = None
        self.timeout = 10  # minutes
        self.db_type: Optional[DatabaseTypeToUse] = db_type
        self.is_managing_cromwell = host is None
        if polling_interval is not None:
            polling_interval = int(polling_interval)
            if polling_interval < 3:
                Logger.warn(
                    f"The polling interval for Cromwell was {polling_interval} seconds, but Janis "
                    f"requires > 3 seconds to ensure metadata is processed correctly"
                )
                polling_interval = None

        self.polling_interval = polling_interval
        self._start_time = None

        self.connectionerrorcount = 0
        self.should_stop = False

        if not self.connect_to_instance:

            # To avoid conflicts between version of Cromwell, we'll find an open
            # port, and allow Cromwell to bind there.

            self.config = None
            self.config_path = os.path.join(confdir, "cromwell.conf")
            self.find_or_generate_config(
                identifier, config=config, config_path=config_path
            )

    def add_callback(self, *args, **kwargs):
        if self._timer_thread is None:
            self._timer_thread = threading.Event()
            self.poll_metadata()

        super().add_callback(*args, **kwargs)

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
            r = request.urlopen(self.url_test())
            return r.code == 200

        except Exception as e:
            Logger.warn(f"Couldn't connect to Cromwell ({self.host}): {repr(e)}")
            return False

    def something_has_happened_to_cromwell(self, rc):
        # Something happpened to cromwell, idk what, but let's report back and say NOT good news

        message = f"Cromwell has exited with returncode={rc}. Your workflow has been suspended"

        for eid, callbacks in self.progress_callbacks.items():
            for cb in callbacks:
                cb(
                    RunModel(
                        id_=RunModel.DEFAULT_ID,
                        submission_id=None,
                        engine_id=eid,
                        status=TaskStatus.SUSPENDED,
                        error=message,
                        execution_dir=None,
                        name=None,
                    )
                )

        # wipe all callbacks
        self.progress_callbacks = {}

        self.stop_engine()

    def get_poll_interval(self):
        """
        Get poll interval in SECONDS
        """
        if self.polling_interval is not None:
            return self.polling_interval

        max_poll = 60  # seconds
        min_poll = 5  # seconds
        falloff = 30 * 60  # seconds

        # https://www.desmos.com/calculator/f5lmjtctfg
        # ~ (max_poll - min_poll)/2 * (tanh(x/falloff - pi) + 1) + min_poll
        #
        # It's just a tanh graph that's been rebiased tanh(x-pi) + 1
        # and then scaled

        x = (DateUtil.now() - (self._start_time or DateUtil.now())).total_seconds()
        return (max_poll - min_poll) / 2 * (
            math.tanh(math.pi * (2 * x / falloff - 1)) + 1
        ) + min_poll

    def start_engine(self, additional_cromwell_options: List[str] = None):

        from janis_assistant.management.configuration import JanisConfiguration

        jc = JanisConfiguration.manager()

        self._start_time = DateUtil.now()
        self.timeout = jc.cromwell.timeout or 10

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

        Logger.info(f"Starting cromwell ({cromwell_loc})...")
        cmd = ["java", "-DLOG_MODE=standard"]

        if jc.cromwell and jc.cromwell.memory:
            cmd.extend(
                [f"-Xmx{jc.cromwell.memory}M", f"-Xms{max(jc.cromwell.memory//2, 1)}M"]
            )

        # if Logger.CONSOLE_LEVEL == LogLevel.VERBOSE:
        #     cmd.append("-DLOG_LEVEL=DEBUG")

        if additional_cromwell_options:
            cmd.extend(additional_cromwell_options)

        self.port = find_free_port()
        self.host = f"127.0.0.1:{self.port}"

        cmd.append(f"-Dwebservice.port={self.port}")
        cmd.append(f"-Dwebservice.interface=127.0.0.1")

        if self.config_path and os.path.exists(self.config_path):
            Logger.debug("Using configuration file for Cromwell: " + self.config_path)
            cmd.append("-Dconfig.file=" + self.config_path)
        cmd.extend(["-jar", cromwell_loc, "server"])

        Logger.debug(f"Starting Cromwell with command: '{' '.join(cmd)}'")
        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            # preexec_fn creates a process group https://stackoverflow.com/a/4791612/
            preexec_fn=os.setsid,
        )
        Logger.info("Cromwell is starting with pid=" + str(self._process.pid))
        Logger.debug(
            "Cromwell will start the HTTP server, reading logs to determine when this occurs"
        )

        self._logfp = open(self.logfile, "a+")
        Logger.info(
            f"Will log Cromwell output to the file: {self.logfile}"
            if bool(self._logfp)
            else "Janis is NOT logging Cromwell output to a file"
        )

        for c in iter(
            self._process.stdout.readline, "b"
        ):  # replace '' with b'' for Python 3

            line = None
            if c:
                line = c.decode("utf-8").rstrip()

            if not line:
                rc = self._process.poll()
                if rc is not None:
                    critical_suffix = f"Last received message '{line}'. "
                    Logger.critical(
                        f"Cromwell has exited with rc={rc}. {critical_suffix}The last lines of the logfile ({self.logfile}):"
                    )
                    Logger.critical("".join(tail(self._logfp, 10)))
                    return False
                continue

            if self._logfp and not self._logfp.closed:
                self._logfp.write(line + "\n")
                self._logfp.flush()
                os.fsync(self._logfp.fileno())

            Logger.debug("Cromwell: " + line)

            # self.stdout.append(str(c))
            if "service started on" in line:
                self.process_id = self._process.pid
                Logger.info(
                    "Service successfully started with pid=" + str(self._process.pid)
                )
                break
            # elif ansi_escape.match():
            #     raise Exception(cd)

        self.is_started = True

        if self._process:
            self._logger = ProcessLogger(
                process=self._process,
                prefix="Cromwell: ",
                logfp=self._logfp,
                # exit_function=self.something_has_happened_to_cromwell,
            )

        return self

    def did_fail(self, rc):
        if rc == 0:
            return

        self.error_message = (
            "Cromwell exited with rc = {rc}, check the engine log for more information"
        )
        self.process_id = None
        self._process = None

    def stop_engine(self):

        if not self.is_started:
            return Logger.debug(
                "Cromwell has already shut down, skipping shut down request"
            )

        if self._logger:
            self._logger.terminate()

        self.should_stop = True

        if self._timer_thread:
            self._timer_thread.set()

        if not self.process_id:
            self.is_started = False
            Logger.info("Janis isn't managing Cromwell, skipping the shutdown")
            return
        Logger.info("Stopping cromwell")
        if self.process_id:
            try:
                process = os.getpgid(int(self.process_id))
                os.killpg(process, signal.SIGTERM)
                Logger.info("Stopped cromwell")
            except Exception as e:
                # can't do
                Logger.warn("Couldn't stop Cromwell process: " + str(e))
        else:
            Logger.warn("Couldn't stop Cromwell process as Janis wasn't managing it")

        Logger.debug("Setting 'cromwell.is_started' to False")
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

        from requests import post

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

        r = post(url, files=files)
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

    def poll_metadata(self):

        if self._timer_thread.is_set() or self.should_stop:
            return

        for engine_id_to_poll in self.progress_callbacks:
            try:
                meta = self.metadata(engine_id_to_poll)
                if meta:
                    for callback in self.progress_callbacks[engine_id_to_poll]:
                        callback(meta)

            except Exception as e:
                Logger.critical(
                    f"Received a critical error ({repr(e)}) when getting metadata for "
                    f"Cromwell task {engine_id_to_poll}, hence terminating task with status=SUSPENDED"
                )
                meta = RunModel(
                    id_=None,
                    submission_id=None,
                    status=TaskStatus.SUSPENDED,
                    engine_id=engine_id_to_poll,
                    name=None,
                    execution_dir=None,
                )
                for callback in self.progress_callbacks[engine_id_to_poll]:
                    callback(meta)

        # call timer again
        time = self.get_poll_interval()
        threading.Timer(time, self.poll_metadata).start()

    @staticmethod
    def resolve_jar(cromwelljar):
        from janis_assistant.management.configuration import JanisConfiguration

        man = JanisConfiguration.manager()
        if not man:
            raise Exception(
                f"No configuration was initialised. This is "
                f"likely an error, and you should raise an issue at {ISSUE_URL}"
            )

        if cromwelljar and os.path.exists(cromwelljar):
            return cromwelljar
        if man.cromwell.jarpath and os.path.exists(man.cromwell.jarpath):
            return man.cromwell.jarpath
        fromenv = EnvVariables.cromwelljar.resolve(False)
        if fromenv and os.path.exists(fromenv):
            return fromenv

        potentials = list(
            reversed(sorted(glob(os.path.join(man.configdir, "cromwell-*.jar"))))
        )

        valid_paths = [p for p in potentials if os.path.exists(p)]
        if len(valid_paths) > 0:
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

            cromwellurl, cromwellfilename = Cromwell.get_latest_cromwell_url()
            Logger.info(
                f"Couldn't find cromwell at any of the usual spots, downloading '{cromwellfilename}' now"
            )
            cromwelljar = os.path.join(man.configdir, cromwellfilename)
            request.urlretrieve(cromwellurl, cromwelljar, show_progress)
            Logger.info(f"Downloaded {cromwellfilename}")

        return cromwelljar

    @staticmethod
    def get_latest_cromwell_url():
        data = request.urlopen(CROMWELL_RELEASES).read()
        releases = json.loads(data)
        asset = releases.get("assets")[0]
        return asset.get("browser_download_url"), asset.get("name")

    def poll_task(self, identifier) -> Optional[TaskStatus]:
        if self.error_message:
            return TaskStatus.FAILED

        url = self.url_poll(identifier=identifier)
        try:
            r = request.urlopen(url)
            data = r.read()
            res = json.loads(data.decode(r.info().get_content_charset("utf-8")))
            return cromwell_status_to_status(res["status"])
        except Exception as e:
            Logger.debug("Error polling Cromwell task:" + str(e))
            return None

    def outputs_task(self, identifier):
        url = self.url_outputs(identifier=identifier)
        try:
            r = request.urlopen(url)
            data = r.read()
            res = json.loads(data.decode(r.info().get_content_charset("utf-8")))
            outs = res.get("outputs")
        except Exception as e:
            return Logger.warn(
                f"Couldn't get outputs with identifier='${identifier}', got error: "
                + str(e)
            )

        if not outs:
            return None
        parsed = [self.parse_output(k, v) for k, v in outs.items()]
        return {out[0]: out[1] for out in parsed}

    @staticmethod
    def parse_output(
        key, value
    ) -> Tuple[str, Union[WorkflowOutputModel, List[WorkflowOutputModel]]]:
        newkey = "".join(key.split(".")[1:])

        fileloc = value
        if isinstance(value, dict):
            fileloc = value["location"]

        if isinstance(fileloc, list):
            return newkey, [Cromwell.parse_output(key, f)[1] for f in fileloc]

        # It's hard to know whether the value we get is a File or just a value,
        # so we'll write it in both values and let Janis figure it out later
        return (
            newkey,
            WorkflowOutputModel(
                id_=newkey,
                original_path=None,
                timestamp=DateUtil.now(),
                new_path=None,
                output_folder=None,
                output_name=None,
                secondaries=None,
                extension=None,
                value=fileloc,
                is_copyable=True,
                submission_id=None,
                run_id=None,
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
        from janis_assistant.management.configuration import JanisConfiguration

        jc = JanisConfiguration.manager()

        if config:
            self.config = config

        elif config_path:
            shutil.copyfile(config_path, self.config_path)

        elif jc.cromwell.configpath:
            shutil.copyfile(jc.cromwell.configpath, self.config_path)

        else:
            self.config: CromwellConfiguration = (
                jc.template.template.engine_config(EngineType.cromwell, jc)
                or CromwellConfiguration()
            )
            if not self.config.system:
                self.config.system = CromwellConfiguration.System()
            self.config.system.cromwell_id = identifier
            self.config.system.cromwell_id_random_suffix = False
            self.config.system.job_shell = "/bin/sh"

        if self.config:

            if self.config.backend:
                if len(self.config.backend.providers) == 1:
                    cnf: CromwellConfiguration.Backend.Provider = first_value(
                        self.config.backend.providers
                    )
                    if not cnf.config.root:
                        cnf.config.root = self.execution_dir
            else:
                self.config.backend = (
                    CromwellConfiguration.Backend.with_new_local_exec_dir(
                        self.execution_dir
                    )
                )

    def raw_metadata(
        self, identifier, expand_subworkflows=True
    ) -> Optional[CromwellMetadata]:
        url = self.url_metadata(
            identifier=identifier, expand_subworkflows=expand_subworkflows
        )

        if not self.last_contacted:
            self.last_contacted = datetime.now()

        Logger.log(f"Getting Cromwell metadata for task '{identifier}' with url: {url}")
        try:
            r = request.urlopen(url)
            self.connectionerrorcount = 0
            self.metadataerrorcount = 0

            self.last_contacted = datetime.now()

            data = r.read()
            jsonobj = json.loads(data.decode(r.info().get_content_charset("utf-8")))

            return CromwellMetadata(jsonobj)

        except request.HTTPError as e:

            if e.code == 404:
                # Usually means Cromwell hasn't loaded properly yet
                return None

            er_message = "An unexpected error occurred: " + str(e)
            try:
                body = e.read().decode()
                jsonobj = json.loads(body) if body else {}
                message = jsonobj.get("message")
                if message:
                    er_message = f"Response when getting Cromwell metadata: {message}"
            except Exception as ee:
                er_message = (
                    f"An additional error occurred while trying to determine the reason"
                    f" why Cromwell metadata failed (internal: {repr(ee)}): {repr(e)})"
                )
            finally:
                Logger.warn("Couldn't get Cromwell metadata: " + str(er_message))
                self.metadataerrorcount += 1
                if self.metadataerrorcount > 5:
                    # we could check the status of the workflow, or we could just suspend it really
                    # 5 consecutive errors
                    raise e
                return None

        except (request.URLError, ConnectionResetError) as e:
            self.connectionerrorcount += 1
            minutes_not_able_to_contact_cromwell = (
                datetime.now() - self.last_contacted
            ).total_seconds() / 60
            if minutes_not_able_to_contact_cromwell > self.timeout:
                message = (
                    f"Janis is receiving a ConnectionResetError when contacting the Cromwell instance "
                    f"({self.host}) {self.connectionerrorcount} times, and has been unable to connect to "
                    f"Cromwell for {minutes_not_able_to_contact_cromwell} minutes. "
                )
                if self.db_type and self.db_type == DatabaseTypeToUse.filebased:
                    ja_config_url = "https://janis.readthedocs.io/en/latest/references/configuration.html#cromwell"
                    message += (
                        "We've seen this issue more frequently when Janis is configuring Cromwell to use the "
                        "file-based database. We recommend configuring Janis to use a MySQL database through the "
                        f"`--mysql` flag, visitng '{ja_config_url}', or raising an issue on GitHub ({GITHUB_URL}) "
                        f"for more information."
                    )
                Logger.warn(message)
                self.something_has_happened_to_cromwell(
                    "last_updated_threshold"
                )  # idk, pick a number
                return None
            if self.connectionerrorcount > 15:
                raise e
            else:
                Logger.warn("Error connecting to cromwell instance: " + repr(e))
            return None

    def metadata(self, identifier, expand_subworkflows=True) -> Optional[RunModel]:
        if self.error_message:
            return RunModel(
                id_=RunModel.DEFAULT_ID,
                submission_id=None,
                error=self.error_message,
                status=TaskStatus.FAILED,
                engine_id=identifier,
                execution_dir=None,
                name=None,
            )

        raw = self.raw_metadata(identifier, expand_subworkflows=expand_subworkflows)
        return raw.standard() if raw else raw

    def terminate_task(self, identifier) -> TaskStatus:
        from time import sleep

        try:

            url = self.url_abort(identifier)

            data = parse.urlencode({}).encode()
            req = request.Request(url, data=data)  # this will make the method "POST"
            r = request.urlopen(req)
            data = r.read()

            Logger.debug("Janis has issued abort request to Cromwell: " + str(data))

            taskstatus = self.poll_task(identifier)
            while taskstatus not in TaskStatus.final_states():
                Logger.debug(
                    f"Task status ({taskstatus}) has not moved to final state after aborting..."
                )
                sleep(1)
                taskstatus = self.poll_task(identifier)

            Logger.info(
                f"Workflow with Cromwell identifier ({identifier} has been terminated ({taskstatus})."
            )

            self.progress_callbacks.pop(identifier)
        except Exception as e:
            raise Exception(f"Failed to abort workflow with id = {identifier} :: {e}")

        return TaskStatus.ABORTED
