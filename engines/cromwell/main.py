import os
import time
import signal
import subprocess


class Cromwell:

    def __init__(self, cromwell_loc="/Users/franklinmichael/broad/cromwell-36.jar"):
        self.cromwell_loc = cromwell_loc
        self.process = None

    def start(self):
        cmd = ["java", "-jar", self.cromwell_loc, "server"]
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, preexec_fn=os.setsid)
        time.sleep(1)
        # print(self.process.stdout.readlines())

    def stop(self):
        if not self.process:
            print("Could not find a cromwell process to end, SKIPPING")
            return

        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        print("\n".join(str(l) for l in self.process.stdout.readlines()))
        # while self.process.poll():
        #     time.sleep(1)

        print("Stopped cromwell")
