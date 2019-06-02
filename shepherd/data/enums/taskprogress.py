from enum import Enum


class ProgressKeys(Enum):
    createOutputStructure = "createOutputStructure"
    saveWorkflow = "saveWorkflow"
    submitWorkflow = "submitWorkflow"
    workflowMovedToFinalState = "workflowMovedToFinalState"
    copiedOutputs = "copiedOutputs"
    savedMetadata = "savedMetadata"
    savedLogs = "savedLogs"

    def __str__(self):
        return self.value
