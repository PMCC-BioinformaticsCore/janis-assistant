import json
import os
import subprocess
import tempfile
from typing import Dict, Any

from runner.engines.engine import Engine, TaskStatus, TaskBase
from runner.utils.logger import Logger


class CWLTool(Engine):

    taskid_to_process = {}

    def __init__(self, options=None):
        self.options = options if options else []

    def start_engine(self):
        Logger.info("Cwltool doesn't run in a server mode, an instance will "
                    "automatically be started when a task is created")

    def stop_engine(self):
        Logger.info(("CWLTool doesn't run in a server mode, an instance will "
                     "be automatically terminated when a task is finished"))

    def create_task(self, source=None, inputs=None, dependencies=None) -> str:
        import uuid
        return str(uuid.uuid4())

    def poll_task(self, identifier) -> TaskStatus:
        if identifier in self.taskid_to_process:
            return TaskStatus.RUNNING
        return TaskStatus.COMPLETED

    def outputs_task(self, identifier) -> Dict[str, Any]:
        pass

    def start_task(self, task: TaskBase):
        task.identifier = self.create_task(None, None, None)

        temps = []
        sourcepath, inputpaths, toolspath = task.source_path, task.input_paths, task.dependencies_path
        if task.source:
            t = tempfile.NamedTemporaryFile(mode="w+t", suffix=".cwl", delete=False)
            t.writelines(task.source)
            t.seek(0)
            temps.append(t)
            sourcepath = t.name

        if task.inputs:
            inputs = []
            for s in task.inputs:
                t = tempfile.NamedTemporaryFile(mode="w+t")
                t.writelines(s)
                t.seek(0)
                inputs.append(t)
                inputpaths = [t.name for t in inputs]
            temps.extend(inputs)

        if task.dependencies:
            # might need to work out where to put these
            t = tempfile.NamedTemporaryFile(mode="w+t")
            t.writelines(task.dependencies)
            temps.append(t)
            toolspath = t.name

        # start cwltool
        cmd = ["cwltool", *self.options]
        if sourcepath: cmd.append(sourcepath)
        if inputpaths:
            if len(inputpaths) > 1:
                raise Exception("CWLTool only accepts 1 input, Todo: Implement inputs merging later")
            cmd.append(inputpaths[0])
        if toolspath: cmd.append(toolspath)

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE)
        Logger.info("CWLTool has started with pid=" + str(process.pid))
        self.taskid_to_process[task.identifier] = process.pid

        for c in iter(process.stderr.readline, 'b'):  # replace '' with b'' for Python 3
            Logger.log("cwltool: " + c.decode("utf-8").strip())
            if b"Final process status is success" in c:
                break
        j = ""
        Logger.log("Process has completed")
        for c in iter(process.stdout.readline, 's'):  # replace '' with b'' for Python 3
            if not c: continue
            j += c.decode("utf-8")
            try:
                json.loads(j)
                break
            except:
                continue
        print("Completed!")
        print(json.loads(j))





