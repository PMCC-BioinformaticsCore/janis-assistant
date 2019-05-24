from enum import Enum


class InfoKeys(Enum):
    taskId = "taskId"
    engineId = "engineId"
    status = "status"
    start = "start"
    environment = "environment"
    validating = "validating"
    engine_tid = "engineTid"

    def __str__(self):
        return self.value


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
