from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class PbsSingularityTemplate(SingularityEnvironmentTemplate):
    def __init__(
        self,
        executionDir: str,
        containerDir: str,
        queue: str = None,
        mail_program=None,
        sendJobEmails=True,
        buildInstructions=f"singularity pull $image docker://${{docker}}",
        singularityLoadInstructions=None,
        max_cores=None,
        max_ram=None,
    ):

        super().__init__(
            mail_program=mail_program,
            containerDir=containerDir,
            buildInstructions=buildInstructions,
            loadInstructions=singularityLoadInstructions,
            max_cores=max_cores,
            max_ram=max_ram,
        )
        self.execution_dir = executionDir
        self.queue = queue
        self.send_job_emails = sendJobEmails

    def cromwell(self):

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(job_shell="/bin/sh"),
            backend=CromwellConfiguration.Backend(
                default="pbs-singularity",
                providers={
                    "pbs-singularity": CromwellConfiguration.Backend.Provider.torque_singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        buildinstructions=self.singularity_build_instructions,
                        send_job_updates=self.send_job_emails,
                        queue=self.queue,
                    )
                },
            ),
        )

        beconfig: CromwellConfiguration.Backend.Provider.Config = config.backend.providers[
            config.backend.default
        ].config
        beconfig.root = self.execution_dir
        beconfig.filesystems = {
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
