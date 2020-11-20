import threading
import os
from datetime import datetime
from typing import IO

from janis_core.utils.logger import Logger


class ProcessLogger(threading.Thread):
    def __init__(self, process, prefix, logfp, error_keyword=None, exit_function=None):
        """

        :param process:
        :param prefix:
        :param logfp:
        :param error_keyword: If this error keyword is found, stop the ProcessLogger and call the exit function
        :param exit_function: A function that is called if the process exits
        """
        threading.Thread.__init__(self)
        self.should_terminate = False
        self.process = process
        self.prefix = prefix
        self.logfp: IO = logfp
        self.rc = None
        self.error_keyword = error_keyword
        self.exit_function = exit_function

        self.last_write = datetime.now()

        self.start()

    def terminate(self):
        self.should_terminate = True
        if self.logfp:
            try:
                self.logfp.flush()
                os.fsync(self.logfp.fileno())
            except Exception as e:
                # This isn't a proper error, there's nothing we could do
                # and doesn't prohibit the rest of the shutdown of Janis.
                Logger.critical("Couldn't flush engine stderr to disk: " + str(e))

    def run(self):
        try:
            for c in iter(
                self.process.stdout.readline, "b"
            ):  # replace '' with b'' for Python 3
                if self.should_terminate:
                    return
                rc = self.process.poll()
                if rc is not None:
                    # process has terminated
                    self.rc = rc
                    print("Process has ended")
                    if self.exit_function:
                        self.exit_function(rc)
                    return
                if not c:
                    continue

                line = c.decode("utf-8").rstrip()

                if not line:
                    continue
                has_error = self.error_keyword and self.error_keyword in line
                # log to debug / critical the self.prefix + line
                (Logger.critical if has_error else Logger.debug)(self.prefix + line)

                should_write = (datetime.now() - self.last_write).total_seconds() > 5

                if self.logfp and not self.logfp.closed:
                    self.logfp.write(line + "\n")
                    if should_write:
                        self.last_write = datetime.now()
                        self.logfp.flush()
                        os.fsync(self.logfp.fileno())

                if has_error:
                    # process has terminated
                    self.rc = rc
                    self.logfp.flush()
                    os.fsync(self.logfp.fileno())

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
