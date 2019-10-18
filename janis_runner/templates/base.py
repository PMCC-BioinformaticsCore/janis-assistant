from abc import ABC, abstractmethod

from janis_core import Logger

from janis_runner.engines.enginetypes import EngineType


class EnvironmentTemplate(ABC):
    def __init__(self, mail_program=None):
        self._mail_program = mail_program

    @abstractmethod
    def engine_config(self, engine: EngineType):
        pass

    def submit_detatched_engine(self, command):
        import subprocess

        subprocess.Popen(command)

    def post_configuration_hook(self, configuration):
        if self._mail_program:
            configuration.notifications.mail_program = self._mail_program
