import os
import time
import signal
import threading
import subprocess

from engines.cromwell.api import CromwellApi


class ProcessLogger(threading.Thread):
    def __init__(self, process, log_output):
        threading.Thread.__init__(self)
        self.should_terminate = False
        self.process = process
        self.log_output = log_output

    def terminate(self):
        self.should_terminate = True

    def run(self):
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if self.should_terminate: return
            if not c: continue
            if self.log_output:
                print(c)


class Cromwell:

    def __init__(self, cromwell_loc="/Users/franklinmichael/broad/cromwell-36.jar", log_output=False):
        self.cromwell_loc = cromwell_loc
        self.log_output = log_output

        self.process = None
        self.logger = None
        self.stdout = []
        self.api = CromwellApi()

    def start(self):
        print("Starting cromwell ...")
        cmd = ["java", "-jar", self.cromwell_loc, "server"]
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid)
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if not c: continue
            if self.log_output:
                print(c)
            # self.stdout.append(str(c))
            if "service started on" in str(c):
                print("Service successfully started")
                break

        if self.process:
            self.logger = ProcessLogger(self.process, self.log_output)
            self.logger.start()

    def watch_output(self):
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if not c: continue
            print(c)

    def stop(self):
        self.logger.terminate()
        if not self.process:
            print("Could not find a cromwell process to end, SKIPPING")
            return
        print("Stopping cromwell")
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        print("Stopped cromwell")


class CromwellTask(threading.Thread):

    def __init__(self, cromwell, source, inputs, dependencies=None, handler=None, should_start=False):
        threading.Thread.__init__(self)
        self.cromwell: Cromwell = cromwell
        self.source = source
        self.inputs = inputs if isinstance(inputs, list) else [inputs]
        self.dependencies = dependencies
        self.handler = handler

        self.task_id = None

        if should_start:
            self.start()

    def start(self):
        print("Creating task")
        self.task_id = self.cromwell.api.create(
            source=self.source,
            inputs=["/Users/franklinmichael/source/shepherd/helloworld.json"],
            dependencies=None
        )
        print("Created task with id: " + self.task_id)
        status = None
        while status != "Succeeded":
            status = self.cromwell.api.poll(self.task_id)
            print(status)
            time.sleep(1)

        if self.handler:
            self.handler(f"Completed: '{self.task_id}'")
        return
