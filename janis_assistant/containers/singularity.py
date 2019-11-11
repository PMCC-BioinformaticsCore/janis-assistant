import os
import subprocess

from typing import Dict

from janis_core import Logger

from janis_assistant.containers.base import Container
from janis_assistant.utils import generate_new_id


class Singularity(Container):

    containerdir: str = None
    buildinstructions: str = None  # replace ${{docker}} and $image
    loadinstructions: str = None

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

        if self.loadinstructions:
            subprocess.check_output(self.loadinstructions, shell=True)

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
                command + [self.container_path(), self.instancename], env=newenv
            )
            Logger.log(out)

            out2 = subprocess.check_output(
                ["singularity", "run", "instance://" + self.instancename]
            )
            Logger.log(out2)

        except subprocess.CalledProcessError as e:
            raise Exception(
                "An error occurred while starting a singularity container: " + str(e)
            )

    def container_path(self):
        import re

        subbed = re.sub("[^A-Za-z0-9._-]", "_", self.container + ".sif")
        return os.path.join(self.containerdir or ".", subbed)

    def stop_container(self):

        cmd = ["singularity", "instance", "stop", self.instancename]
        return subprocess.check_output(cmd)

    def exec_command(self, command):
        cmd = ["singularity", "run", self.instancename, *command]
        cmd.extend(command) if isinstance(command, list) else cmd.append(command)

        return subprocess.check_output(cmd)

    def ensure_downloaded(self):

        if self.loadinstructions:
            subprocess.check_output(self.loadinstructions, shell=True)

        pathed_container = self.container_path()

        if os.path.exists(pathed_container):
            return True

        if Singularity.buildinstructions:
            command = Singularity.buildinstructions.replace(
                "$image", pathed_container
            ).replace("${{docker}}", self.container)
        else:
            command = [
                "singularity",
                "pull",
                pathed_container,
                "docker://" + self.container,
            ]
        try:
            build_result = subprocess.check_output(command)
            Logger.log(build_result)
        except subprocess.CalledProcessError as e:
            Logger.log_ex(e)
