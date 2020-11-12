import json
import os
import re
import subprocess
from typing import Dict, Any

from janis_core import LogLevel
from janis_core.types.data_types import is_python_primitive
from janis_core.utils.logger import Logger
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.run import RunModel
from janis_assistant.data.models.workflowjob import RunJobModel
from janis_assistant.engines.cwltool.cwltoolconfiguation import CWLToolConfiguration
from janis_assistant.engines.engine import Engine, TaskStatus
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.utils import ProcessLogger
from janis_assistant.utils.dateutils import DateUtil


class CWLToolLogger(ProcessLogger):

    statusupdateregex = re.compile("INFO \[(.*)\] (.+)$")

    def __init__(self, sid: str, process, logfp, metadata_callback, exit_function=None):
        self.sid = sid

        self.error = None
        self.metadata_callback = metadata_callback
        self.outputs = None
        self.workflow_scope = []
        super().__init__(
            process=process, prefix="cwltool", logfp=logfp, exit_function=exit_function
        )

    def run(self):
        finalstatus = None
        iserroring = False

        try:
            for c in iter(self.process.stderr.readline, "b"):
                if self.should_terminate:
                    return

                line = None
                if c:
                    line = c.decode("utf-8").rstrip()

                if not line:
                    if self.process.poll() is not None:
                        finalstatus = TaskStatus.ABORTED
                        Logger.warn(
                            f"CWLTool finished with rc={self.process.returncode} but janis "
                            f"was unable to capture the workflow status. Marking as aborted"
                        )
                        break
                    continue

                if self.logfp and not self.logfp.closed:
                    self.logfp.write(line + "\n")
                    self.logfp.flush()
                    os.fsync(self.logfp.fileno())

                lowline = line.lower().lstrip()
                if lowline.startswith("error"):
                    Logger.critical("cwltool: " + line)
                    iserroring = True

                elif lowline.startswith("warn"):
                    iserroring = False
                    Logger.warn("cwltool: " + line)

                elif lowline.startswith("info"):
                    iserroring = False
                    Logger.info("cwltool: " + line)
                    self.process_metadataupdate_if_match(line)

                else:
                    Logger.debug("cwltool: " + line)

                if iserroring:
                    self.error = (self.error or "") + "\n" + line

                if "final process status is" in lowline:
                    if "fail" in line.lower():
                        finalstatus = TaskStatus.FAILED
                    elif "success" in line.lower():
                        finalstatus = TaskStatus.COMPLETED
                    else:
                        finalstatus = TaskStatus.ABORTED
                    break

            j = ""
            Logger.info("Process has completed")
            if finalstatus == TaskStatus.COMPLETED:
                for c in iter(self.process.stdout.readline, "s"):
                    if not c:
                        continue
                    line = c.decode("utf-8").rstrip()
                    Logger.debug(line)
                    if self.logfp and not self.logfp.closed:
                        self.logfp.write(line + "\n")
                        self.logfp.flush()
                        os.fsync(self.logfp.fileno())
                    j += line
                    try:
                        self.outputs = json.loads(j)
                        break
                    except:
                        continue

            if self.error:
                Logger.critical("Janis detected a CWLTool error: " + self.error)

            Logger.info(
                "CWLTool detected transition to terminal status: " + str(finalstatus)
            )
            self.terminate()
            if self.exit_function:
                self.exit_function(self, finalstatus)

        except KeyboardInterrupt:
            self.should_terminate = True
            print("Detected keyboard interrupt")
            # raise
        except Exception as e:
            print("Detected another error")
            raise e

    def process_metadataupdate_if_match(self, line):
        match = self.statusupdateregex.match(line)
        if not match:
            return

        name, action = match.groups()

        s = name.split(" ")
        if len(s) == 0 or len(s) > 2:
            return Logger.critical("Unsure how to handle metadata update: " + str(line))

        component = s[0]
        stepname = s[1] if len(s) > 1 else None

        status = None
        parentid = "_".join(self.workflow_scope) or None

        if component == "workflow":
            if action == "start":
                if stepname:  # stepname is empty for root workflow
                    self.workflow_scope.append(stepname)
                    stepname = None
                    status = TaskStatus.RUNNING
            elif action == "completed success":
                if len(self.workflow_scope) > 0:
                    self.workflow_scope.pop(0)
                status = TaskStatus.COMPLETED

        elif component == "step":
            if action == "start":
                status = TaskStatus.RUNNING
            elif action == "completed success":
                status = TaskStatus.COMPLETED

        if not status:
            return

        if not stepname:
            # return WorkflowModel
            return

        jid = f"{parentid}_{stepname}" if parentid else stepname

        start = DateUtil.now() if status == TaskStatus.RUNNING else None
        finish = DateUtil.now() if status == TaskStatus.COMPLETED else None

        job = RunJobModel(
            submission_id=None,
            run_id=self.sid,
            id_=jid,
            parent=parentid,
            name=stepname,
            status=status,
            attempt=None,
            shard=None,
            start=start,
            finish=finish,
            backend="local",
            batchid="",
            cached=False,
            container=None,
            stderr=self.logfp.name,
            stdout=None,
            analysis=None,
            script=None,
            cpu=None,
            memory=None,
        )

        self.metadata_callback(self, job)


class CWLTool(Engine):
    def __init__(
        self,
        execution_dir: str,
        logfile=None,
        identifier: str = "cwltool",
        config: CWLToolConfiguration = None,
    ):
        super().__init__(
            identifier, EngineType.cwltool, logfile=logfile, execution_dir=execution_dir
        )
        self.process = None
        self._logger = None

        self.taskmeta = {}

        self.config = None
        self.find_or_generate_config(config)

    def find_or_generate_config(self, config: CWLToolConfiguration):
        from janis_assistant.data.models.preparedjob import PreparedJob

        job = PreparedJob.instance()

        if config:
            self.config = config
        else:
            self.config = (
                job.template.template.engine_config(EngineType.cwltool, job)
                or CWLToolConfiguration()
            )

    def test_connection(self):
        return bool(self.process_id)

    def start_engine(self):
        Logger.log(
            "Cwltool doesn't run in a server mode, an instance will "
            "automatically be started when a task is created"
        )
        return self

    def stop_engine(self):

        # we're going to abort!
        if self.process_id:
            Logger.info("Received stop_engine request for CWLTool")
            try:
                import signal

                os.kill(self.process_id, signal.SIGTERM)
            except Exception as e:
                Logger.critical("Couldn't terminate CWLTool as " + str(e))

        else:
            Logger.critical("Couldn't terminate CWLTool as there was no process ID")

        return self

    def poll_task(self, identifier) -> TaskStatus:
        return self.taskmeta.get("status", TaskStatus.PROCESSING)

    def outputs_task(self, identifier) -> Dict[str, Any]:

        outs = self.taskmeta.get("outputs")

        if not outs:
            return {}

        retval: Dict[str, WorkflowOutputModel] = {}
        for k, o in outs.items():
            retval.update(self.process_potential_out(identifier, k, o))

        return retval

    @staticmethod
    def process_potential_out(run_id, key, out):

        if isinstance(out, list):
            outs = [CWLTool.process_potential_out(run_id, key, o) for o in out]
            ups = {}
            for o in outs:
                for k, v in o.items():
                    if k not in ups:
                        ups[k] = []
                    ups[k].append(v)
            return ups

        updates = {}
        if out is None:
            return {}

        if is_python_primitive(out):
            updates[key] = WorkflowOutputModel(
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

        elif "path" in out:
            updates[key] = WorkflowOutputModel(
                submission_id=None,
                run_id=run_id,
                id_=key,
                is_copyable=True,
                value=out["path"],
                original_path=None,
                timestamp=DateUtil.now(),
                new_path=None,
                output_folder=None,
                output_name=None,
                secondaries=None,
                extension=None,
            )
            for s in out.get("secondaryFiles", []):
                path = s["path"]
                ext = path.rpartition(".")[-1]
                newk = f"{key}_{ext}"
                updates[newk] = WorkflowOutputModel(
                    submission_id=None,
                    run_id=run_id,
                    id_=newk,
                    value=path,
                    original_path=None,
                    is_copyable=True,
                    timestamp=DateUtil.now(),
                    new_path=None,
                    output_folder=None,
                    output_name=None,
                    secondaries=None,
                    extension=None,
                )

        return updates

    def terminate_task(self, identifier) -> TaskStatus:
        """
        This CWLTool implementation is not super great. It should start the process and issue an async task
        to watch out for progress and eventually report back to the sqlite database. Then when 'terminate_task'
        is called, it could kill this process (eventually self.pid | self._process) and cleanup the metadata.

        :param identifier:
        :return:
        """
        self.stop_engine()
        self.taskmeta["status"] = TaskStatus.ABORTED
        return TaskStatus.ABORTED

    def metadata(self, identifier) -> RunModel:
        """
        So CWLTool doesn't really have a metadata thing. See the 'terminate_task' description, but this
        implementation should instead create a thread to watch for process, and write metadata back to sqlite.
        Then this method could just read from the sqlite database.

        :param identifier:
        :return:
        """
        return RunModel(
            id_=identifier,
            engine_id=identifier,
            execution_dir=None,
            submission_id=None,
            name=identifier,
            status=self.taskmeta.get("status"),
            # start=self.taskmeta.get("start"),
            # finish=self.taskmeta.get("finish"),
            # outputs=meta.get("outputs") or [],
            jobs=list(self.taskmeta.get("jobs", {}).values()),
            error=self.taskmeta.get("error"),
            # executiondir=None,
        )

    def start_from_paths(self, wid, source_path: str, input_path: str, deps_path: str):

        from janis_assistant.data.models.preparedjob import PreparedJob

        jobfile = PreparedJob.instance()

        self.taskmeta = {
            "start": DateUtil.now(),
            "status": TaskStatus.PROCESSING,
            "jobs": {},
        }
        config: CWLToolConfiguration = self.config

        if Logger.CONSOLE_LEVEL == LogLevel.VERBOSE:
            config.debug = True

        config.disable_color = True

        # more options
        if not config.tmpdir_prefix:
            config.outdir = self.execution_dir + "/"
            config.tmpdir_prefix = self.execution_dir + "/"
            config.leave_tmpdir = True

        if jobfile.call_caching_enabled:
            config.cachedir = os.path.join(self.execution_dir, "cached/")

        cmd = config.build_command_line(source_path, input_path)

        Logger.debug("Running command: '" + " ".join(cmd) + "'")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE
        )
        self.taskmeta["status"] = TaskStatus.RUNNING
        Logger.info("CWLTool has started with pid=" + str(process.pid))
        self.process_id = process.pid

        self._logger = CWLToolLogger(
            wid,
            process,
            logfp=open(self.logfile, "a+"),
            metadata_callback=self.task_did_update,
            exit_function=self.task_did_exit,
        )

        return wid

    def task_did_exit(self, logger: CWLToolLogger, status: TaskStatus):
        Logger.debug("CWLTool fired 'did exit'")
        self.taskmeta["status"] = status
        self.taskmeta["finish"] = DateUtil.now()
        self.taskmeta["outputs"] = logger.outputs

        if status != TaskStatus.COMPLETED:
            js: Dict[str, RunJobModel] = self.taskmeta.get("jobs")
            for j in js.values():
                if j.status != TaskStatus.COMPLETED:
                    j.status = status

        if logger.error:
            self.taskmeta["error"] = logger.error

        for callback in self.progress_callbacks.get(logger.sid, []):
            callback(self.metadata(logger.sid))

    def task_did_update(self, logger: CWLToolLogger, job: RunJobModel):
        Logger.debug(f"Updated task {job.id_} with status={job.status}")
        self.taskmeta["jobs"][job.id_] = job

        for callback in self.progress_callbacks.get(logger.sid, []):
            callback(self.metadata(logger.sid))
