from typing import Dict
from janis_runner.containers.base import Container


class Singularity(Container):
    def __init__(
        self,
        container: str,
        environment_variables: Dict[str, str] = None,
        bindpoints: Dict[str, str] = None,
        exposedports: Dict[int, int] = None,
    ):
        super().__init__(
            container=container,
            environment_variables=environment_variables,
            bindpoints=bindpoints,
            exposedports=exposedports,
        )

        self.dockerid = None

    def start_container(self):
        import subprocess

        command = ["singularity", "run", "-d"]

        # if self.environment_variables:
        #     command.extend(f"-e{k}={v}" for k, v in self.environment_variables.items())

        if self.bindpoints:
            command.extend(f"-B{v}:{k}" for k, v in self.bindpoints.items())

        if self.exposedports:
            command.extend(
                [
                    "--net",
                    # "--network=none",
                    "--network-args",
                    *[f'"portmap={v}:{k}/tcp"' for k, v in self.exposedports.items()],
                ]
            )

        try:
            self.dockerid = subprocess.check_output(command + [self.container])

        except subprocess.CalledProcessError as e:
            raise Exception(
                "An error occurred while starting a singularity container: " + str(e)
            )

    def stop_container(self):
        import subprocess

        cmd = [
            "singularity",
            "stop",
            self.dockerid,
            "&&",
            "singularity",
            "rm",
            self.dockerid,
        ]
        rc = subprocess.check_output(cmd)

    def exec_command(self, command):
        pass
