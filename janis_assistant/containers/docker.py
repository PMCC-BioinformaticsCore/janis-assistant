import subprocess
from typing import Dict

from janis_core import Logger

from janis_assistant.containers.base import Container, ContainerType


class Docker(Container):
    def __init__(
        self,
        container: str,
        environment_variables: Dict[str, str] = None,
        bindpoints: Dict[str, str] = None,
        exposedports: Dict[int, int] = None,
        instancename: str = None,
        **kwargs,  # Extra kwargs to throw away
    ):
        super().__init__(
            container=container,
            environment_variables=environment_variables,
            bindpoints=bindpoints,
            exposedports=exposedports,
            instancename=instancename,
        )

        self.dockerid = None

    @staticmethod
    def get_container_type():
        return ContainerType.docker

    @staticmethod
    def test_available_by_getting_version() -> str:
        try:
            return subprocess.check_output(["docker", "-v"]).decode()
        except subprocess.CalledProcessError as e:
            raise Container.ContainerEnvNotFound("docker", e)

    def start_container(self):
        command = ["docker", "run", "-d"]

        if self.environment_variables:
            command.extend(f"-e{k}={v}" for k, v in self.environment_variables.items())

        if self.bindpoints:
            command.extend(f"-v{v}:{k}" for k, v in self.bindpoints.items())

        if self.exposedports:
            for k, v in self.exposedports.items():
                command.extend(["-p", f"{k}:{v or k}"])

        if self.instancename:
            command.extend(["--name", self.instancename])

        command.append(self.container)
        Logger.info("Starting docker with command: " + str(" ".join(command)))

        try:
            self.dockerid = subprocess.check_output(command).decode("ascii").strip()
            self.running = True

        except subprocess.CalledProcessError as e:
            raise Exception(
                f"An error occurred while starting a docker container: {e} ({e.output or e.stderr})"
            )

    def stop_container(self):

        if not self.dockerid:
            return Logger.info(
                f"Skipping end docker container '{self.container}' as no dockerID was found"
            )
        cmd = f"docker stop {self.dockerid}; docker rm -f {self.dockerid}"

        try:
            Logger.info("Stopping docker with command: " + str(cmd))

            subprocess.check_output(cmd, shell=True)
            self.running = False

        except subprocess.CalledProcessError as e:
            Logger.critical(
                f"An error occurred when trying to stop the container '{self.container}'. "
                f"You may need to stop this manually with: '{cmd}'. Error: " + str(e)
            )
            raise e

    def exec_command(self, command):

        cmd = ["docker", "exec", "-i", self.dockerid]
        requiresshell = not isinstance(command, list)
        cmd.extend(command) if isinstance(command, list) else cmd.append(command)
        try:
            Logger.info("Executing command: " + " ".join(cmd))
            val = (
                subprocess.check_output(cmd, shell=requiresshell)
                .decode("ascii")
                .strip()
            )

        except subprocess.CalledProcessError as e:
            Logger.critical(f"Docker exec_command failed '{e}': {e.output or e.stderr}")

            # check the logs
            try:
                logs_command = ["docker", "logs", self.dockerid]
                Logger.info("Checking docker logs: " + " ".join(logs_command))
                Logger.debug(subprocess.check_output(logs_command))
            except:
                Logger.critical(f"Failed to get logs for container {self.dockerid}")

            return (str(e), e.returncode)

        return val.strip() if val else val, 0

    def ensure_downloaded(self):
        # meh it'll download itself anyway
        return
