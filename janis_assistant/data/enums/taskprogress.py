from enum import Enum


class ProgressKeys(Enum):
    start = "start"
    createOutputStructure = "createOutputStructure"
    saveWorkflow = "saveWorkflow"
    submitWorkflow = "submitWorkflow"
    workflowMovedToFinalState = "workflowMovedToFinalState"
    copiedOutputs = "copiedOutputs"
    savedMetadata = "savedMetadata"
    savedLogs = "savedLogs"
    cleanedUp = "cleanedUp"

    def __str__(self):
        return self.value
