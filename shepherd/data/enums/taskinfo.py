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
