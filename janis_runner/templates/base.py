from abc import ABC, abstractmethod
from typing import Type

from janis_core import Logger

from janis_runner.containers.base import Container
from janis_runner.containers.docker import Docker
from janis_runner.engines.enginetypes import EngineType


class EnvironmentTemplate(ABC):
    def __init__(self, mail_program=None, containertype: Type[Container] = Docker):
        self._mail_program = mail_program
        self.containertype = containertype

    @abstractmethod
    def engine_config(self, engine: EngineType):
        pass

    def submit_detatched_resume(self, wid, command):
        import subprocess

        Logger.info("Starting Janis resume with " + str(command))
        subprocess.Popen(
            command,
            close_fds=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # subprocess.Popen(command) # Keep process open

    def post_configuration_hook(self, configuration):
        if self._mail_program:
            configuration.notifications.mail_program = self._mail_program

        configuration.container = self.containertype
