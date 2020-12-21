from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.cwltool.cwltoolconfiguation import CWLToolConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import (
    EnvironmentTemplate,
    SingularityEnvironmentTemplate,
)


class LocalTemplate(EnvironmentTemplate):

    ignore_init_keys = []

    def __init__(self):
        super().__init__()

    def cromwell(self, job: PreparedJob):
        hashing_strategy = job.cromwell.call_caching_method or "cached-copy"

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(),
            # equiv to providers.Local.config.filesystems.local.caching.hashing-strategy = "path+modtime"
            backend=CromwellConfiguration.Backend(
                providers={
                    "Local": CromwellConfiguration.Backend.Provider(
                        config=CromwellConfiguration.Backend.Provider.Config(
                            filesystems={
                                "local": CromwellConfiguration.Backend.Provider.Config.Filesystem(
                                    caching=CromwellConfiguration.Backend.Provider.Config.Filesystem.Caching(
                                        hashing_strategy=hashing_strategy,
                                    ),
                                )
                            }
                        )
                    )
                }
            ),
        )

        if job.call_caching_enabled:
            config.call_caching = CromwellConfiguration.CallCaching(enabled=True)

        return config

    def cwltool(self, job):
        config = CWLToolConfiguration()

        return config

    def engine_config(self, engine: EngineType, job: PreparedJob):

        if engine == EngineType.cromwell:
            return self.cromwell(job=job)

        elif engine == EngineType.cwltool:
            return self.cwltool(job=job)

        # Returning none will allow the engine to run with no config
        return None


class LocalSingularityTemplate(SingularityEnvironmentTemplate):

    ignore_init_keys = [
        "can_run_in_foreground",
        "run_in_background",
        "container_build_instructions",
        "singularity_load_instructions",
    ]

    def __init__(
        self,
        container_dir,
        singularity_load_instructions=None,
        container_build_instructions=f"singularity pull $image docker://${{docker}}",
        mail_program: str = None,
    ):
        """

        :param container_dir: Location where to save and execute containers from
        :param singularity_load_instructions: Ensure singularity with this command executed in shell
        :param container_build_instructions: Instructions for building singularity, it's recommended to not touch this setting.
        :param mail_program: Mail program to pipe email to, eg: 'sendmail -t'
        """
        super().__init__(
            mail_program=mail_program,
            container_dir=container_dir,
            build_instructions=container_build_instructions,
            load_instructions=singularity_load_instructions,
        )

    def cromwell(self, janis_configuration):
        hashing_strategy = janis_configuration.cromwell.call_caching_method or "file"

        config = CromwellConfiguration(
            backend=CromwellConfiguration.Backend(
                providers={
                    "Local": CromwellConfiguration.Backend.Provider.singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        buildinstructions=self.singularity_build_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        call_caching_method=hashing_strategy,
                    )
                }
            )
        )

        if janis_configuration.call_caching_enabled:
            config.call_caching = CromwellConfiguration.CallCaching(enabled=True)

        return config

    def cwltool(self, janis_configuration):

        config = CWLToolConfiguration()
        config.singularity = True

        return config

    def engine_config(self, engine: EngineType, janis_configuration):

        if engine == EngineType.cromwell:
            return self.cromwell(janis_configuration=janis_configuration)

        elif engine == EngineType.cwltool:
            return self.cwltool(janis_configuration=janis_configuration)

        # Returning none will allow the engine to run with no config
        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
