import threading
import os
from typing import IO

from janis_core.utils.logger import Logger


class ProcessLogger(threading.Thread):
    def __init__(self, process, prefix, logfp, exit_function=None):
        threading.Thread.__init__(self)
        self.should_terminate = False
        self.process = process
        self.prefix = prefix
        self.logfp: IO = logfp
        self.rc = None
        self.exit_function = exit_function

        self.start()

    def terminate(self):
        self.should_terminate = True
        if self.logfp:
            self.logfp.flush()
            os.fsync(self.logfp.fileno())

    def run(self):
        try:
            for c in iter(
                self.process.stdout.readline, "b"
            ):  # replace '' with b'' for Python 3
                if self.should_terminate:
                    return
                if not c:
                    continue
                line = c.decode("utf-8").rstrip()
                if not line:
                    continue
                Logger.log(self.prefix + line)
                if self.logfp and not self.logfp.closed:
                    self.logfp.write(line + "\n")
                    self.logfp.flush()
                    os.fsync(self.logfp.fileno())

                rc = self.process.poll()
                if rc is not None:
                    # process has terminated
                    self.rc = rc
                    print("Process has ended")
                    if self.exit_function:
                        self.exit_function(rc)
                    return
        except KeyboardInterrupt:
            self.should_terminate = True
            print("Detected keyboard interrupt")
            # raise
        except Exception as e:
            print("Detected another error")
            raise e
