from typing import Union, List

from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class SlurmSingularityTemplate(SingularityEnvironmentTemplate):
    def __init__(
        self,
        container_dir: str,
        execution_dir: str = None,
        queues: Union[str, List[str]] = None,
        mail_program=None,
        send_job_emails=True,
        catch_slurm_errors=False,
        build_instructions=f"singularity pull $image docker://${{docker}}",
        singularity_load_instructions=None,
        limit_resources=False,
        max_cores=None,
        max_ram=None,
    ):
        """
        :param execution_dir: A location where the execution should take place
        :param container_dir: Location where to save and execute containers from
        :param queues: A single or list of queues that work should be submitted to
        :param mail_program: Mail program to pipe email to, eg: 'sendmail -t'
        :param catch_slurm_errors: Catch Slurm errors (like OOM or walltime)
        :param send_job_emails: (requires JanisConfiguration.notifications.email to be set) Send emails for mail types END
        :param build_instructions: Instructions for building singularity, it's recommended to not touch this setting.
        :param singularity_load_instructions: Ensure singularity with this command executed in shell
        :param limit_resources: Limit resources with singularity using cgroups (REQUIRES ROOT)
        :param max_cores: Maximum number of cores a task can request
        :param max_ram: Maximum amount of ram (GB) that a task can request
        """

        super().__init__(
            mail_program=mail_program,
            container_dir=container_dir,
            build_instructions=build_instructions,
            load_instructions=singularity_load_instructions,
            max_cores=max_cores,
            max_ram=max_ram,
        )
        self.execution_dir = execution_dir
        self.queues = queues or []
        self.send_slurm_emails = send_job_emails
        self.catch_slurm_errors = catch_slurm_errors
        self.limitResources = limit_resources

    def cromwell(self):
        from janis_assistant.management.configuration import JanisConfiguration

        slurm_email = None
        if self.send_slurm_emails:
            slurm_email = JanisConfiguration.manager().notifications.email

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(
                job_shell="/bin/sh", file_hash_cache=True
            ),
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
        if self.execution_dir:
            beconfig.root = self.execution_dir
        beconfig.filesystems = {
            "local": {
                "localization": ["hard-link", "cached-copy", "soft-link", "copy"]
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
