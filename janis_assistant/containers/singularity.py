import os
from typing import Dict
from janis_assistant.containers.base import Container
from janis_assistant.utils import generate_new_id


class Singularity(Container):
    def __init__(
        self,
        container: str,
        environment_variables: Dict[str, str] = None,
        bindpoints: Dict[str, str] = None,
        exposedports: Dict[int, int] = None,
        instancename: str = None,
    ):
        super().__init__(
            container=container,
            environment_variables=environment_variables,
            bindpoints=bindpoints,
            exposedports=exposedports,
            instancename=instancename,
        )

        self.dockerid = None

    def start_container(self):
        import subprocess

        command = ["singularity", "instance", "start"]

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
            newenv = os.environ
            for k in self.environment_variables:
                newenv["SINGULARITYENV_" + k] = self.environment_variables[k]

            if not self.instancename:
                self.instancename = generate_new_id(set())

            out = subprocess.check_output(
                command + [self.container, self.instancename], env=newenv
            )

            out2 = subprocess.check_output(
                ["singularity", "run", "instance://" + self.instancename]
            )

        except subprocess.CalledProcessError as e:
            raise Exception(
                "An error occurred while starting a singularity container: " + str(e)
            )

    def stop_container(self):
        import subprocess

        cmd = ["singularity", "instance", "stop", self.instancename]
        return subprocess.check_output(cmd)

    def exec_command(self, command):
        pass
