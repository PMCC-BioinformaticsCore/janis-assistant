import json
import os
import shutil
import subprocess
import tempfile
from typing import Dict, Any

from shepherd.engines.engine import Engine, TaskStatus, TaskBase
from shepherd.utils.logger import Logger


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
            if len(task.inputs) > 1:
                raise Exception("CWLTool currently only supports 1 input file")
            for s in task.inputs:
                if isinstance(s, dict):
                    import ruamel.yaml
                    s = ruamel.yaml.dump(s, default_flow_style=False)
                t = tempfile.NamedTemporaryFile(mode="w+t", suffix=".yml")
                t.writelines(s)
                t.seek(0)
                inputs.append(t)
                inputpaths = [t.name for t in inputs]
            temps.extend(inputs)

        if task.dependencies:
            # might need to work out where to put these

            tmp_container = tempfile.tempdir + "/"
            tmpdir = tmp_container + "tools/"
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
            os.mkdir(tmpdir)
            for (f, d) in task.dependencies:
                with open(tmp_container + f, "w+") as q:
                    q.write(d)
            temps.append(tmpdir)

        # start cwltool
        cmd = ["cwltool", *self.options]
        if sourcepath: cmd.append(sourcepath)
        if inputpaths:
            if len(inputpaths) > 1:
                raise Exception("CWLTool only accepts 1 input, Todo: Implement inputs merging later")
            cmd.append(inputpaths[0])
        # if toolspath: cmd.extend(["--basedir", toolspath])

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE)
        Logger.log("Running command: '" + " ".join(cmd) + "'")
        Logger.info("CWLTool has started with pid=" + str(process.pid))
        self.taskid_to_process[task.identifier] = process.pid

        for c in iter(process.stderr.readline, 'b'):  # replace '' with b'' for Python 3
            line = c.decode("utf-8").rstrip()
            if not line.strip(): continue
            Logger.log("cwltool: " + line)
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
        Logger.info("Workflow has completed execution")
        process.terminate()

        print(json.loads(j))


        # close temp files
        Logger.log(f"Closing {len(temps)} temp files")
        for t in temps:
            if hasattr(t, "close"):
                t.close()
            if isinstance(t, str):
                if os.path.exists(t) and os.path.isdir(t):
                    shutil.rmtree(t)
                else:
                    os.remove(t)
