from abc import ABC, abstractmethod
from typing import Type

from janis_core import Logger

from janis_assistant.containers.base import Container
from janis_assistant.containers.docker import Docker
from janis_assistant.containers.singularity import Singularity
from janis_assistant.engines.enginetypes import EngineType


class EnvironmentTemplate(ABC):
    def __init__(
        self,
        mail_program=None,
        max_cores=None,
        max_ram=None,
        containertype: Type[Container] = Docker,
    ):
        self._mail_program = mail_program
        self.containertype = containertype
        self.max_cores = max_cores
        self.max_ram = max_ram

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

    def post_configuration_hook(self, configuration):
        if self._mail_program:
            configuration.notifications.mail_program = self._mail_program

        if self.max_cores and not configuration.environment.max_cores:
            configuration.environment.max_cores = self.max_cores

        if self.max_ram and not configuration.environment.max_ram:
            configuration.environment.max_ram = self.max_ram

        configuration.container = self.containertype


class SingularityEnvironmentTemplate(EnvironmentTemplate):
    def __init__(
        self,
        mail_program: str,
        containerDir: str,
        loadInstructions=None,
        buildInstructions=f"singularity pull $image docker://${{docker}}",
        max_cores=None,
        max_ram=None,
    ):
        super().__init__(
            mail_program=mail_program,
            containertype=Singularity,
            max_cores=max_cores,
            max_ram=max_ram,
        )
        self.singularity_load_instructions = loadInstructions
        self.singularity_container_dir = containerDir
        self.singularity_build_instructions = buildInstructions

        Logger.log(
            f"Setting Singularity: containerdir={containerDir}, loadinstructions={loadInstructions}"
        )
        # little bit hacky
        Singularity.containerdir = containerDir
        Singularity.loadinstructions = loadInstructions
        Singularity.buildinstructions = buildInstructions

        pass
