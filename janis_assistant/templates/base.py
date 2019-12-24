from abc import ABC, abstractmethod
from typing import Type, Optional, List

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

    def submit_detatched_resume(self, wid: str, command: List[str], loglocation: str):
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

    def prejanis_hook(self) -> Optional[str]:
        """
        Before Janis starts to run a workflow, this block of code gets executed.
        If a string is returned, it is executed in the current environment. This
        block is also executed on a resume.

        This might be a good place to load relevant dependencies

        :return: str: bash script to run
        """
        pass

    def postjanis_success_hook(self) -> Optional[str]:
        """
        After a workflow successfully completes, this block of code is executed.
        This might be a good place to unload dependencies if required.

        :return: str: bash script to run
        """
        pass

    def postjanis_failure_hook(self) -> Optional[str]:
        """
        If a workflow fails to complete, this block of code is executed.
        This might be a good place to unload dependencies if required.

        :return: str: bash script to run
        """
        pass


class SingularityEnvironmentTemplate(EnvironmentTemplate, ABC):
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
