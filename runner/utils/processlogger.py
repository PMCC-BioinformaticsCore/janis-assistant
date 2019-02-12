import threading

from runner.utils.logger import Logger


class ProcessLogger(threading.Thread):
    def __init__(self, process, prefix):
        threading.Thread.__init__(self)
        self.should_terminate = False
        self.process = process
        self.prefix = prefix
        self.start()

    def terminate(self):
        self.should_terminate = True

    def run(self):
        try:
            for c in iter(self.process.stdout.readline, 'b'):  # replace '' with b'' for Python 3
                if self.should_terminate: return
                if not c: continue
                Logger.log(self.prefix + c.decode("utf-8").strip())
        except KeyboardInterrupt:
            self.should_terminate = True
            print("Detected keyboard interrupt")
            # raise
        except Exception:
            print("Detected another error")
            # raise
