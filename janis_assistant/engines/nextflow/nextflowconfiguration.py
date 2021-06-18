import os
import stat
import shutil
import subprocess
from typing import Optional, Dict, List, Any
from janis_assistant.management.configuration import JanisConfigurationNextflow
from janis_assistant.data.models.preparedjob import PreparedJob
from janis_core.utils.logger import Logger


class NextflowConfiguration:
    EXECUTABLE = "nextflow"
    DOWNLOAD_EXEC_URL = "https://get.nextflow.io"

    def __init__(self,
                 job: PreparedJob,
                 process_executor: Optional[str] = None,
                 docker: Optional[bool] = True,
                 singularity: Optional[bool] = False,
                 queue: Optional[str] = None):

        self.job = job
        self.process_executor = process_executor
        self.singularity = singularity
        self.docker = docker
        self.queue = queue
        self.executable_path = self.resolve_executable(job.nextflow, job.config_dir)

    def build_command_line(self, source_path: str, input_path: str, nextflow_log_filename: str, host: str, port: int):

        config_path = os.path.join("nextflow.config")
        config_values = {}

        # config_values["executor.dumpInterval"] = self._to_nexflow_string("1min")
        # we only want one or the other and we want to prioritise singularity
        if self.singularity:
            config_values["singularity.enabled"] = self._to_nexflow_string(self.singularity)
            config_values["singularity.autoMounts"] = self._to_nexflow_string(True)
        else:
            config_values["docker.enabled"] = self._to_nexflow_string(self.docker)

        if self.process_executor is not None:
            config_values["process.executor"] = self._to_nexflow_string(self.process_executor)

        if self.queue is not None:
            config_values["process.queue"] = self._to_nexflow_string(self.queue)

        config_lines = [f"{key} = {value}" for key, value in config_values.items()]
        with open(config_path, "w") as f:
            f.write("\n".join(config_lines))

        cmd = [
            self.executable_path,
            "-C", config_path,
            "-log", nextflow_log_filename,
            "run", source_path,
            "-params-file", input_path,
            "-ansi-log", 'false',
            "-with-weblog", f"http://{host}:{port}",
            "-resume"
        ]

        return cmd

    def _to_nexflow_string(self, val: Any):
        if type(val) == bool:
            return str(val).lower()
        if type(val) == str:
            return f"'{val}'"

        return val

    @classmethod
    def executable_exists(cls):
        return shutil.which(cls.EXECUTABLE)

    @classmethod
    def resolve_executable(cls, janis_nextflow_config: JanisConfigurationNextflow, janis_config_dir: str) -> str:
        """
        Resolve the path to the Nextflow executable file

        :param janis_nextflow_config:
        :type janis_nextflow_config:
        :param janis_config_dir:
        :type janis_config_dir:
        :return:
        :rtype:
        """
        # Order of checks:
        # 1. Check if Nextflow executable path is provided in config
        # 2. Check if Nextflow executable can be found in $PATH env variable
        # 3. Attempt to download Nextflow executable
        path = janis_nextflow_config.executable

        if path is None:
            path = cls.executable_exists()

        if path is None:
            path = os.path.join(janis_config_dir, cls.EXECUTABLE)
            if not os.path.exists(path):
                path = None

        # Now, try to download online
        if path is None:
            try:
                Logger.info("Downloading Nextflow executable")
                path = os.path.join(janis_config_dir, cls.EXECUTABLE)
                process = subprocess.Popen(f"cd {janis_config_dir} && curl -s {cls.DOWNLOAD_EXEC_URL} | bash",
                                           shell=True)
                process.wait()
                if os.path.exists(path):
                    st = os.stat(path)
                    os.chmod(path, st.st_mode | stat.S_IEXEC)
                else:
                    raise
            except Exception as e:
                raise Exception("Failed to download Nextflow executable")

        Logger.info(f"Nextflow executable path: {path}")

        return path