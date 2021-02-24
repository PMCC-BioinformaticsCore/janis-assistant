from enum import Enum


class EngineType(Enum):
    cromwell = "cromwell"
    cwltool = "cwltool"
    toil = "toil"
    nextflow = "nextflow"

    def __str__(self):
        return self.value

    @staticmethod
    def engines():
        return [EngineType.cwltool.value, EngineType.cromwell.value, EngineType.nextflow.value]
