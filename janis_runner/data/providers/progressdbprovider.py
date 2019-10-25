from janis_runner.data.enums import ProgressKeys
from janis_runner.data.keyvaluedbproviderbase import KvDB


class ProgressDbProvider(KvDB):
    attributes_to_persist = [k.value for k in ProgressKeys]

    def __init__(self, dblocation):
        super().__init__(dblocation, tablename="progress")

        if "start" not in self.kvdb:
            for k in self.attributes_to_persist:
                self.__setattr__(k, False)

            self.start = True
            self.createOutputStructure = False
            self.saveWorkflow = False
            self.submitWorkflow = False
            self.workflowMovedToFinalState = False
            self.copiedOutputs = False
            self.savedMetadata = False
            self.savedLogs = False
            self.cleanedUp = False
