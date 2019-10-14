from abc import ABC, abstractmethod

from janis_core import Logger

from janis_runner.engines.enginetypes import EngineType


class EnvironmentTemplate(ABC):
    def __init__(self, mail_program=None):
        self.mail_program = mail_program

    @abstractmethod
    def engine_config(self, engine: EngineType):
        pass

    def preengine_code(self, engine: EngineType):
        pass
