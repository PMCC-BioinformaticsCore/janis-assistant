from typing import Union, List

from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class SlurmSingularityTemplate(SingularityEnvironmentTemplate):
    def __init__(
        self,
        executionDir: str,
        containerDir: str,
        queues: Union[str, List[str]] = None,
        mail_program=None,
        sendSlurmEmails=True,
        catchSlurmErrors=False,
        buildInstructions=f"singularity pull $image docker://${{docker}}",
        singularityLoadInstructions=None,
        limitResources=False,
        max_cores=None,
        max_ram=None,
    ):
        """
        :param executionDir: A location where the execution should take place
        :param containerDir: Location where to save and execute containers from
        :param queues: A single or list of queues that work should be submitted to
        :param mail_program: Mail program to pipe email to, eg: 'sendmail -t'
        :param catchSlurmErrors: Catch Slurm errors (like OOM or walltime)
        :param sendSlurmEmails: (requires JanisConfiguration.notifications.email to be set) Send emails for mail types END
        :param buildInstructions: Instructions for building singularity, it's recommended to not touch this setting.
        :param singularityLoadInstructions: Ensure singularity with this command executed in shell
        :param limitResources: Limit resources with singularity using cgroups (REQUIRES ROOT)
        :param max_cores: Maximum number of cores a task can request
        :param max_ram: Maximum amount of ram (GB) that a task can request
        """

        super().__init__(
            mail_program=mail_program,
            containerDir=containerDir,
            buildInstructions=buildInstructions,
            loadInstructions=singularityLoadInstructions,
            max_cores=max_cores,
            max_ram=max_ram,
        )
        self.execution_dir = executionDir
        self.queues = queues or []
        self.send_slurm_emails = sendSlurmEmails
        self.catch_slurm_errors = catchSlurmErrors
        self.limitResources = limitResources

    def cromwell(self):
        from janis_assistant.management.configuration import JanisConfiguration

        slurm_email = None
        if self.send_slurm_emails:
            slurm_email = JanisConfiguration.manager().notifications.email

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(job_shell="/bin/sh"),
            backend=CromwellConfiguration.Backend(
                default="slurm-singularity",
                providers={
                    "slurm-singularity": CromwellConfiguration.Backend.Provider.slurm_singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        buildinstructions=self.singularity_build_instructions,
                        jobemail=slurm_email,
                        jobqueues=self.queues,
                        afternotokaycatch=self.catch_slurm_errors,
                        limit_resources=self.limitResources,
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
