from typing import Union, List
from janis_runner.engines.enginetypes import EngineType
from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_runner.templates.base import EnvironmentTemplate


class SlurmSingularityTemplate(EnvironmentTemplate):
    def __init__(
        self,
        executionDir: str,
        containerDir: str,
        queues: Union[str, List[str]] = None,
        mail_program=None,
        email=None,
        catchSlurmErrors=False,
        buildInstructions=f"singularity pull $image docker://${{docker}}",
        singularityLoadInstructions=None,
    ):

        super().__init__(mail_program=mail_program)
        self.execution_dir = executionDir
        self.queues = queues or []
        self.email = email
        self.container_dir = containerDir
        self.catch_slurm_errors = catchSlurmErrors
        self.build_instructions = buildInstructions
        self.singularity_load_instructions = singularityLoadInstructions

    def cromwell(self):

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(job_shell="/bin/sh"),
            backend=CromwellConfiguration.Backend(
                default="slurm-singularity",
                providers={
                    "slurm-singularity": CromwellConfiguration.Backend.Provider.slurm_singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        singularitycontainerdir=self.container_dir,
                        buildinstructions=self.build_instructions,
                        jobemail=self.email,
                        jobqueues=self.queues,
                        afternotokaycatch=self.catch_slurm_errors,
                        limit_resources=True,
                    )
                },
            ),
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

    def engine_config(self, engine: EngineType):
        if engine == EngineType.cromwell:
            return self.cromwell()

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
