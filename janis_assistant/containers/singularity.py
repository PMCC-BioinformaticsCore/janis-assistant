import os
import subprocess

from typing import Dict

from janis_core import Logger

from janis_assistant.containers.base import Container, ContainerType
from janis_assistant.utils import generate_new_id, ProcessLogger


class Singularity(Container):
    def __init__(
        self,
        container: str,
        environment_variables: Dict[str, str] = None,
        bindpoints: Dict[str, str] = None,
        exposedports: Dict[int, int] = None,
        instancename: str = None,
        containerdir: str = None,
        **kwargs,  # Extra kwargs to throw away
    ):
        super().__init__(
            container=container,
            environment_variables=environment_variables,
            bindpoints=bindpoints,
            exposedports=exposedports,
            instancename=instancename,
        )

        self.containerdir = containerdir
        self.dockerid = None
        self.run_logger = None

    @staticmethod
    def get_container_type():
        return ContainerType.singularity

    @staticmethod
    def get_build_instructions_for(containerlocation: str, docker: str):
        return ["singularity", "pull", containerlocation, "docker://" + docker]

    @staticmethod
    def test_available_by_getting_version() -> str:
        try:
            version = subprocess.check_output(["singularity", "--version"]).decode()
            import re

            # require Singularity 3.x.x
            match = re.search("(\d+\.\d+\.\d+)", version)

            if not match:
                raise Exception(f"Couldn't interpret singularity version {version}")
            parsed_version = match.group(0)
            if int(parsed_version[0]) < 3:
                raise Exception(
                    f"Unsupported singularity version {version}, expected 3.x.x"
                )

            return version
        except subprocess.CalledProcessError as e:
            raise Container.ContainerEnvNotFound("singularity", e)

    def start_container(self):

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

            out = subprocess.check_output(command, env=newenv, stderr=subprocess.STDOUT)
            Logger.debug(f"Singularity output: {out}")

            singrun = ["singularity", "run", "instance://" + self.instancename]
            Logger.debug("Started container, now running " + str(singrun))
            startprocess = subprocess.Popen(
                singrun, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            self.run_logger = ProcessLogger(
                startprocess,
                prefix="mysql: ",
                logfp=None,
                exit_function=self.runlogger_didexit,
            )

        except subprocess.CalledProcessError as e:
            raise Exception(
                f"An error occurred while starting a singularity container: {str(e)}.\n\tOutput: {str(e.output)}"
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
            Logger.critical("Singularity exec_command failed")
            return str(e), e.returncode

    def ensure_downloaded(self):

        pathed_container = self.container_path()

        if os.path.exists(pathed_container):
            return True

        command = self.get_build_instructions_for(pathed_container, self.container)
        Logger.info(
            "Couldn't find singularity container, building with: " + " ".join(command)
        )
        try:
            build_result = subprocess.check_output(command)
            Logger.debug(build_result)
        except subprocess.CalledProcessError as e:
            Logger.log_ex(e)
