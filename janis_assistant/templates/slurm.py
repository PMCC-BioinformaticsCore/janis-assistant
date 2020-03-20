from typing import Union, List

from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class SlurmSingularityTemplate(SingularityEnvironmentTemplate):

    ignore_init_keys = [
        "build_instructions",
        "singularity_load_instructions",
        "limit_resources",
        "max_cores",
        "max_ram",
        "can_run_in_foreground",
        "run_in_background",
    ]

    def __init__(
        self,
        container_dir: str,
        execution_dir: str = None,
        queues: Union[str, List[str]] = None,
        mail_program=None,
        send_job_emails=False,
        catch_slurm_errors=True,
        build_instructions=f"singularity pull $image docker://${{docker}}",
        singularity_load_instructions=None,
        max_cores=None,
        max_ram=None,
        can_run_in_foreground=True,
        run_in_background=False,
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
            can_run_in_foreground=can_run_in_foreground,
            run_in_background=run_in_background,
        )
        self.execution_dir = execution_dir
        self.queues = queues or []
        self.send_job_emails = send_job_emails
        self.catch_slurm_errors = catch_slurm_errors

    def cromwell(self, janis_configuration):

        job_email = None
        if self.send_job_emails:
            job_email = janis_configuration.notifications.email

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(
                job_shell="/bin/sh"  # file_hash_cache=True
            ),
            backend=CromwellConfiguration.Backend(
                default="slurm-singularity",
                providers={
                    "slurm-singularity": CromwellConfiguration.Backend.Provider.slurm_singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        buildinstructions=self.singularity_build_instructions,
                        jobemail=job_email,
                        jobqueues=self.queues,
                        afternotokaycatch=self.catch_slurm_errors,
                        call_caching_method=janis_configuration.cromwell.call_caching_method,
                    )
                },
            ),
        )

        beconfig: CromwellConfiguration.Backend.Provider.Config = config.backend.providers[
            config.backend.default
        ].config
        if self.execution_dir:
            beconfig.root = self.execution_dir

        if janis_configuration.call_caching_enabled:
            config.call_caching = CromwellConfiguration.CallCaching(enabled=True)

        return config

    def engine_config(self, engine: EngineType, janis_configuration):
        if engine == EngineType.cromwell:
            return self.cromwell(janis_configuration)

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
