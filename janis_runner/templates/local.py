import ruamel.yaml
from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_runner.templates.base import EnvironmentTemplate
from janis_runner.engines.enginetypes import EngineType


class LocalTemplate(EnvironmentTemplate):

    default_recipes = {}

    def __init__(self, executionDir=None, mailProgram: str = None):
        super().__init__(mail_program=mailProgram)

        self.executionDir = executionDir

    def cromwell(self):
        return None

    def engine_config(self, engine: EngineType):
        return None
