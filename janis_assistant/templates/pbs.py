from typing import List, Union

from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class PbsSingularityTemplate(SingularityEnvironmentTemplate):

    ignore_init_keys = [
        "can_run_in_foreground",
        "run_in_background",
        "build_instructions",
        "singularity_load_instructions",
        "max_cores",
        "max_ram",
        "max_duration",
        "intermediate_execution_dir",
    ]

    def __init__(
        self,
        container_dir: str = None,
        intermediate_execution_dir: str = None,
        queues: Union[str, List[str]] = None,
        mail_program=None,
        send_job_emails=False,
        catch_pbs_errors=True,
        build_instructions=f"singularity pull $image docker://${{docker}}",
        singularity_load_instructions=None,
        max_cores=None,
        max_ram=None,
        max_duration=None,
        can_run_in_foreground=True,
        run_in_background=False,
    ):
        """
        :param intermediate_execution_dir: A location where the execution should take place
        :param container_dir: Location where to save and execute containers to, this will also look at the env variables 'CWL_SINGULARITY_CACHE', 'SINGULARITY_TMPDIR'
        :param queues: A queue that work should be submitted to
        :param mail_program: Mail program to pipe email to, eg: 'sendmail -t'
        :param send_job_emails: (requires JanisConfiguration.notifications.email to be set) Send emails for mail types END
        :param build_instructions: Instructions for building singularity, it's recommended to not touch this setting.
        :param singularity_load_instructions: Ensure singularity with this command executed in shell
        :param max_cores: Maximum number of cores a task can request
        :param max_ram: Maximum amount of ram (GB) that a task can request
        :param max_duration: Maximum amount of time in seconds (s) that a task can request
        """

        super().__init__(
            mail_program=mail_program,
            container_dir=container_dir,
            build_instructions=build_instructions,
            load_instructions=singularity_load_instructions,
            max_cores=max_cores,
            max_ram=max_ram,
            max_duration=max_duration,
            can_run_in_foreground=can_run_in_foreground,
            run_in_background=run_in_background,
        )
        self.intermediate_execution_dir = intermediate_execution_dir
        self.queues = queues
        self.send_job_emails = send_job_emails
        self.catch_pbs_errors = catch_pbs_errors

    def cromwell(self, job: PreparedJob):

        job_email = None
        if self.send_job_emails:
            job_email = job.notifications.email

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(
                job_shell="/bin/sh"  # file_hash_cache=True
            ),
            backend=CromwellConfiguration.Backend(
                default="pbs-singularity",
                providers={
                    "pbs-singularity": CromwellConfiguration.Backend.Provider.torque_singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        buildinstructions=self.singularity_build_instructions,
                        jobemail=job_email,
                        queues=self.queues,
                        afternotokaycatch=self.catch_pbs_errors,
                        call_caching_method=job.cromwell.call_caching_method,
                    )
                },
            ),
        )

        beconfig: CromwellConfiguration.Backend.Provider.Config = (
            config.backend.providers[config.backend.default].config
        )
        if self.intermediate_execution_dir:
            beconfig.root = self.intermediate_execution_dir

        if job.call_caching_enabled:
            config.call_caching = CromwellConfiguration.CallCaching(enabled=True)

        return config

    def engine_config(self, engine: EngineType, job: PreparedJob):
        if engine == EngineType.cromwell:
            return self.cromwell(job=job)

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
