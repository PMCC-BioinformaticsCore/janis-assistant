import subprocess
from typing import Union, List

from janis_core import Logger

from janis_runner.engines.enginetypes import EngineType
from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_runner.templates.base import EnvironmentTemplate


class PeterMacTestTemplate(EnvironmentTemplate):
    def __init__(
        self,
        executionDir: str,
        queues: Union[str, List[str]] = "prod_med,prod",
        containerDir="/config/binaries/singularity/containers_devel/janis/",
        singularityVersion="3.4.0",
    ):

        super().__init__(mail_program="sendmail -t")
        self.execution_dir = executionDir
        self.queues = queues or ["prod_short", "prod_med", "prod"]
        self.container_dir = containerDir
        self.singularity_version = singularityVersion

    def cromwell(self):

        joined_queued = (
            ",".join(self.queues) if isinstance(self.queues, list) else self.queues
        )

        config = CromwellConfiguration(
            backend=CromwellConfiguration.Backend(
                default="pmac",
                providers={
                    "pmac": CromwellConfiguration.Backend.Provider.singularity(
                        singularityloadinstructions="module load singularity/"
                        + self.singularity_version,
                        singularitycontainerdir=self.container_dir,
                        buildinstructions=(
                            f"sbatch -p {joined_queued} --wait \
                              --wrap 'docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}}) "
                            f"&& image={self.container_dir}/$docker_subbed.sif && singularity pull $image docker://${{docker}}'"
                        ),
                    )
                },
            )
        )

        backend: CromwellConfiguration.Backend.Provider.Config = config.backend.providers[
            config.backend.default
        ].config
        backend.root = self.execution_dir
        backend.filesystems = {
            "local": {
                "localization": ["cached-copy", "hard-link", "soft-link", "copy"]
            },
            # "caching": {"hashing-strategy": "path+modtime"},
        }

        return config

    def submit_detatched_engine(self, command):
        q = self.queues or "prod_short"
        jq = ", ".join(q) if isinstance(q, list) else q
        jc = " ".join(command) if isinstance(command, list) else command
        newcommand = ["sbatch", "-p", jq, "--time", "30", "--wrap", jc]
        Logger.info("Starting command: " + str(newcommand))
        rc = subprocess.call(
            newcommand,
            close_fds=True,
            # stdout=subprocess.DEVNULL,
            # stderr=subprocess.DEVNULL,
        )
        if rc != 0:
            raise Exception(f"Couldn't submit janis-monitor, non-zero exit code ({rc})")

    def engine_config(self, engine: EngineType):
        if engine == EngineType.cromwell:
            return self.cromwell()

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
