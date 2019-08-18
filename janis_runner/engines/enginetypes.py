from enum import Enum


class EngineType(Enum):
    cromwell = "cromwell"
    cwltool = "cwltool"
    toil = "toil"

    def __str__(self):
        return self.value
