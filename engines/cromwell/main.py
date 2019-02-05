import os
import time
import signal
import threading
import subprocess


class ProcessLogger(threading.Thread):
    def __init__(self, process):
        threading.Thread.__init__(self)
        self.process = process

    def run(self):
        print("starting " + self.name)
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if not c: continue
            print(c)


class Cromwell:

    def __init__(self, cromwell_loc="/Users/franklinmichael/broad/cromwell-36.jar"):
        self.cromwell_loc = cromwell_loc
        self.process = None
        self.stdout = []

    def start(self):
        cmd = ["java", "-jar", self.cromwell_loc, "server"]
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid)
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if not c: continue
            print(c)
            # self.stdout.append(str(c))
            if "service started on" in str(c):
                print("Service successfully started")
                break

        if self.process:
            ProcessLogger(self.process).start()

    def watch_output(self):
        for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
            if not c: continue
            print(c)

    def stop(self):
        if not self.process:
            print("Could not find a cromwell process to end, SKIPPING")
            return
        print("Stopping cromwell")
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        print("Stopped cromwell")
