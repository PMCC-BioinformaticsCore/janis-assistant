import os
import subprocess

from typing import Dict

from janis_core import Logger

from janis_assistant.containers.base import Container
from janis_assistant.utils import generate_new_id, ProcessLogger


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
        self.run_logger = None

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
                    "--network=none",
                    "--network-args",
                    *[f"portmap={v}:{k}/tcp" for k, v in self.exposedports.items()],
                ]
            )

        try:
            newenv = os.environ
            for k in self.environment_variables:
                newenv["SINGULARITYENV_" + k] = str(self.environment_variables[k])

            if not self.instancename:
                self.instancename = generate_new_id(set())

            command.extend([self.container_path(), self.instancename])
            Logger.info("Starting singularity with command: " + " ".join(command))
            out = subprocess.check_output(command, env=newenv)
            Logger.log(out)

            startprocess = subprocess.Popen(
                ["singularity", "run", "instance://" + self.instancename],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.run_logger = ProcessLogger(
                startprocess,
                prefix="mysql: ",
                logfp=None,
                exit_function=self.runlogger_didexit,
            )

        except subprocess.CalledProcessError as e:
            raise Exception(
                "An error occurred while starting a singularity container: " + str(e)
            )

    def runlogger_didexit(self, rc):
        Logger.info("singularity run finished with code: " + str(rc))
        self.run_logger = None

    def container_path(self):
        import re

        subbed = re.sub("[^A-Za-z0-9._-]", "_", self.container + ".sif")
        return os.path.join(self.containerdir or ".", subbed)

    def stop_container(self):
        try:
            Logger.info(f"Stopping mysql container '{self.instancename}'")
            if self.run_logger:
                self.run_logger.terminate()
            cmd = ["singularity", "instance", "stop", self.instancename]
            return subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            Logger.critical(
                f"Couldn't stop singularity instance '{self.instancename}': {e}"
            )

    def exec_command(self, command):
        cmd = ["singularity", "run", "instance://" + self.instancename]
        cmd.extend(command) if isinstance(command, list) else cmd.append(command)

        try:
            val = subprocess.check_output(cmd)
            return val.strip().decode() if val else val, 0
        except subprocess.CalledProcessError as e:
            Logger.critical("Docker exec_command failed")
            return str(e), e.returncode

    def ensure_downloaded(self):

        if self.loadinstructions:
            subprocess.check_output(self.loadinstructions, shell=True)

        pathed_container = self.container_path()

        if os.path.exists(pathed_container):
            return True

        # if Singularity.buildinstructions:
        #     command = Singularity.buildinstructions.replace(
        #         "$image", pathed_container
        #     ).replace("${docker}", self.container)
        # else:
        command = [
            "singularity",
            "pull",
            pathed_container,
            "docker://" + self.container,
        ]
        Logger.info(
            "Couldn't find singularity container, building with: " + " ".join(command)
        )
        try:
            build_result = subprocess.check_output(command)
            Logger.log(build_result)
        except subprocess.CalledProcessError as e:
            Logger.log_ex(e)
