from typing import List, Union

from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class PbsSingularityTemplate(SingularityEnvironmentTemplate):
    def __init__(
        self,
        container_dir: str,
        execution_dir: str = None,
        queues: Union[str, List[str]] = None,
        mail_program=None,
        send_job_emails=True,
        catch_pbs_errors=True,
        build_instructions=f"singularity pull $image docker://${{docker}}",
        singularity_load_instructions=None,
        max_cores=None,
        max_ram=None,
    ):
        """
        :param execution_dir: A location where the execution should take place
        :param container_dir: Location where to save and execute containers from
        :param queues: A queue that work should be submitted to
        :param mail_program: Mail program to pipe email to, eg: 'sendmail -t'
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
        )
        self.execution_dir = execution_dir
        self.queues = queues
        self.send_job_emails = send_job_emails
        self.catch_pbs_errors = catch_pbs_errors

    def cromwell(self):

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(
                job_shell="/bin/sh", file_hash_cache=True
            ),
            backend=CromwellConfiguration.Backend(
                default="pbs-singularity",
                providers={
                    "pbs-singularity": CromwellConfiguration.Backend.Provider.torque_singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        buildinstructions=self.singularity_build_instructions,
                        send_job_updates=self.send_job_emails,
                        queues=self.queues,
                        afternotokaycatch=self.catch_pbs_errors,
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
