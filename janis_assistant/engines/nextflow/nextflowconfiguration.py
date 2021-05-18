import os
from typing import Optional, Dict, List, Any


class NextflowConfiguration:
    def __init__(self,
                 process_executor: Optional[str] = None,
                 docker: Optional[bool] = True,
                 singularity: Optional[bool] = False):

        self.process_executor = process_executor
        self.singularity = singularity
        self.docker = docker

    def build_command_line(self, source_path: str, input_path: str, nextflow_log_filename: str):
        # cmd = ["nextflow", "-C", config_path, "run", source_path, '-params-file', input_path]

        config_path = os.path.join("nextflow.config")
        config_values = {}
        # config_values = {
        #     "docker.enabled": "true"
        # }

        # config_values["executor.dumpInterval"] = self._to_nexflow_string("1min")
        # we only want one or the other and we want to prioritise singularity
        if self.singularity:
            config_values["singularity.enabled"] = self._to_nexflow_string(self.singularity)
            config_values["singularity.autoMounts"] = self._to_nexflow_string(True)
        else:
            config_values["docker.enabled"] = self._to_nexflow_string(self.docker)

        if self.process_executor is not None:
            config_values["process.executor"] = self._to_nexflow_string(self.process_executor)

        config_lines = [f"{key} = {value}" for key, value in config_values.items()]
        with open(config_path, "w") as f:
            f.write("\n".join(config_lines))

        cmd = [
            "nextflow",
            "-C", config_path,
            "-log", nextflow_log_filename,
            "run", source_path,
            "-params-file", input_path,
            "-ansi-log", 'false',
            "-resume"
        ]

        return cmd

    def _to_nexflow_string(self, val: Any):
        if type(val) == bool:
            return str(val).lower()
        if type(val) == str:
            return f"'{val}'"

        return val