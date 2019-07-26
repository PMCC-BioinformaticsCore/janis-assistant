from enum import Enum


class InfoKeys(Enum):
    taskId = "taskId"
    engineId = "engineId"
    fileschemeId = "fileschemeId"

    name = "name"
    status = "status"
    start = "start"
    finish = "finish"
    environment = "environment"
    validating = "validating"
    engine_tid = "engineTid"

    def __str__(self):
        return self.value
