from abc import ABC, abstractmethod
from typing import Type, Optional, List

from janis_core import Logger

from janis_assistant.containers.base import Container
from janis_assistant.containers.docker import Docker
from janis_assistant.containers.singularity import Singularity
from janis_assistant.engines.enginetypes import EngineType


class EnvironmentTemplate(ABC):

    ignore_init_keys = ["can_run_in_foreground", "run_in_background"]

    def __init__(
        self,
        mail_program=None,
        max_cores=None,
        max_ram=None,
        can_run_in_foreground=True,
        run_in_background=None,
        containertype: Type[Container] = Docker,
    ):
        self._mail_program = mail_program
        self.containertype = containertype
        self.max_cores = max_cores
        self.max_ram = max_ram

        self.can_run_in_foreground = can_run_in_foreground
        self.run_in_background = run_in_background

    @abstractmethod
    def engine_config(self, engine: EngineType, janis_configuration):
        pass

    def submit_detatched_resume(
        self,
        wid: str,
        command: List[str],
        scriptdir: str,
        logsdir: str,
        config,
        capture_output: bool = False,
    ):
        import subprocess

        Logger.info(
            "Starting Janis in the background with: "
            + (" ".join(command) if isinstance(command, list) else str(command))
        )

        try:
            if capture_output:
                out = subprocess.check_output(
                    command, close_fds=True, stderr=subprocess.STDOUT
                )
                Logger.info(out.decode().strip())
            else:
                subprocess.Popen(
                    command,
                    close_fds=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
        except subprocess.CalledProcessError as e:
            Logger.critical(
                f"Couldn't submit janis-monitor, non-zero exit code ({e.returncode})"
            )
            raise e

    def post_configuration_hook(self, configuration):
        if self._mail_program:
            configuration.notifications.mail_program = self._mail_program

        if self.max_cores and not configuration.environment.max_cores:
            configuration.environment.max_cores = self.max_cores

        if self.max_ram and not configuration.environment.max_ram:
            configuration.environment.max_ram = self.max_ram

        if self.run_in_background is not None:
            configuration.run_in_background = self.run_in_background

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
        container_dir: str,
        load_instructions=None,
        build_instructions=f"singularity pull $image docker://${{docker}}",
        max_cores=None,
        max_ram=None,
        can_run_in_foreground=True,
        run_in_background=False,
    ):
        super().__init__(
            mail_program=mail_program,
            containertype=Singularity,
            max_cores=max_cores,
            max_ram=max_ram,
            can_run_in_foreground=can_run_in_foreground,
            run_in_background=run_in_background,
        )
        self.singularity_load_instructions = load_instructions
        self.singularity_container_dir = container_dir
        self.singularity_build_instructions = build_instructions

        Logger.log(
            f"Setting Singularity: containerdir={container_dir}, loadinstructions={load_instructions}"
        )
        # little bit hacky
        Singularity.containerdir = container_dir
        Singularity.loadinstructions = load_instructions
        Singularity.buildinstructions = build_instructions

        pass
