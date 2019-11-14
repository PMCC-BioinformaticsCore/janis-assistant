import json
import os
import re
import subprocess
from typing import Dict, Any

from janis_core.utils.logger import Logger
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.workflow import WorkflowModel
from janis_assistant.data.models.workflowjob import WorkflowJobModel
from janis_assistant.engines.engine import Engine, TaskStatus
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.utils import ProcessLogger
from janis_assistant.utils.dateutil import DateUtil


class CWLToolLogger(ProcessLogger):

    statusupdateregex = re.compile("INFO \[(.*)\] (.+)$")

    def __init__(self, wid: str, process, logfp, metadata_callback, exit_function=None):
        self.wid = wid

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
                if not c:
                    continue

                line = c.decode("utf-8").strip()
                if not line:
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
                    Logger.log("cwltool: " + line)

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

                elif self.process.poll() is not None:
                    finalstatus = TaskStatus.ABORTED
                    Logger.warn(
                        f"CWLTool finished with rc={self.process.returncode} but janis "
                        f"was unable to capture the workflow status. Marking as aborted"
                    )
                    break

            j = ""
            Logger.log("Process has completed")
            if finalstatus == TaskStatus.COMPLETED:
                for c in iter(self.process.stdout.readline, "s"):
                    if not c:
                        continue
                    j += c.decode("utf-8")
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

        job = WorkflowJobModel(
            jid=jid,
            parentjid=parentid,
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
        )

        self.metadata_callback(self, job)


class CWLTool(Engine):
    def __init__(
        self, logfile=None, identifier: str = "cwltool", options=None, watch=True
    ):
        super().__init__(identifier, EngineType.cwltool, logfile=logfile, watch=True)
        self.options = options if options else []
        self.process = None
        self._logger = None

        self.taskmeta = {}

    def test_connection(self):
        return bool(self.process_id)

    def start_engine(self):
        Logger.info(
            "Cwltool doesn't run in a server mode, an instance will "
            "automatically be started when a task is created"
        )
        return self

    def stop_engine(self):

        # we're going to abort!
        if self.process_id:
            import signal

            os.kill(self.process_id, signal.SIGTERM)

        else:
            Logger.critical("Couldn't terminate CWLTool as there was no processID")

        return self

    def create_task(self, source=None, inputs=None, dependencies=None) -> str:
        import uuid

        print(self.id())

        return str(uuid.uuid4())

    def poll_task(self, identifier) -> TaskStatus:
        return self.taskmeta.get("status", TaskStatus.PROCESSING)

    def outputs_task(self, identifier) -> Dict[str, Any]:

        outs = self.taskmeta.get("outputs")

        if not outs:
            return {}

        retval: Dict[str, WorkflowOutputModel] = {}
        for k, o in outs.items():
            retval.update(self.process_potential_out(k, o))

        return retval

    @staticmethod
    def process_potential_out(key, out):

        if isinstance(out, list):
            outs = [CWLTool.process_potential_out(key, o) for o in out]
            ups = {}
            for o in outs:
                for k, v in o.items():
                    if k not in ups:
                        ups[k] = []
                    ups[k].append(v)
            return ups

        updates = {}
        if "path" in out:
            updates[key] = WorkflowOutputModel(
                tag=key,
                original_path=out["path"],
                timestamp=DateUtil.now(),
                new_path=None,
                tags=None,
                prefix=None,
                secondaries=None,
                extension=None,
            )
        for s in out.get("secondaryFiles", []):
            path = s["path"]
            ext = path.rpartition(".")[-1]
            newk = f"{key}_{ext}"
            updates[newk] = WorkflowOutputModel(
                tag=newk,
                original_path=path,
                timestamp=DateUtil.now(),
                new_path=None,
                tags=None,
                prefix=None,
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

    def metadata(self, identifier) -> WorkflowModel:
        """
        So CWLTool doesn't really have a metadata thing. See the 'terminate_task' description, but this
        implementation should instead create a thread to watch for process, and write metadata back to sqlite.
        Then this method could just read from the sqlite database.

        :param identifier:
        :return:
        """
        return WorkflowModel(
            identifier,
            name=identifier,
            status=self.taskmeta.get("status"),
            start=self.taskmeta.get("start"),
            finish=self.taskmeta.get("finish"),
            # outputs=meta.get("outputs") or [],
            jobs=list(self.taskmeta.get("jobs", {}).values()),
            error=self.taskmeta.get("error"),
            # executiondir=None,
        )

    def start_from_paths(
        self, wid, source_path: str, input_path: str, deps_path: str, execution_dir: str
    ):
        self.taskmeta = {
            "start": DateUtil.now(),
            "status": TaskStatus.PROCESSING,
            "jobs": {},
        }

        cmd = ["cwltool", *self.options, "--disable-color"]

        # more options
        if execution_dir:
            cmd.extend(["--outdir", execution_dir])

        cmd.append(source_path)

        if input_path:
            cmd.append(input_path)

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE
        )
        self.taskmeta["status"] = TaskStatus.RUNNING
        Logger.log("Running command: '" + " ".join(cmd) + "'")
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
        Logger.log("CWLTool fired 'did exit'")
        self.taskmeta["status"] = status
        self.taskmeta["finish"] = DateUtil.now()
        self.taskmeta["outputs"] = logger.outputs

        if status != TaskStatus.COMPLETED:
            js: Dict[str, WorkflowJobModel] = self.taskmeta.get("jobs")
            for j in js.values():
                if j.status != TaskStatus.COMPLETED:
                    j.status = status

        if logger.error:
            self.taskmeta["error"] = logger.error

    def task_did_update(self, logger: CWLToolLogger, job: WorkflowJobModel):
        Logger.info(f"Updated task {job.jid} with status={job.status}")
        self.taskmeta["jobs"][job.jid] = job
