import ruamel.yaml
from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.templates.base import (
    EnvironmentTemplate,
    SingularityEnvironmentTemplate,
)
from janis_assistant.engines.enginetypes import EngineType


class LocalTemplate(EnvironmentTemplate):
    def __init__(self, executionDir=None, mailProgram: str = None):
        """
        :param executionDir: A location where the execution should take place
        :param mailProgram: Mail program to pipe email to, eg: 'sendmail -t'
        """
        super().__init__(mail_program=mailProgram)

        self.executionDir = executionDir

    def cromwell(self):
        if not self.executionDir:
            return None

        config = CromwellConfiguration(
            backend=CromwellConfiguration.Backend.with_new_local_exec_dir(
                self.executionDir
            )
        )

        return config

    def engine_config(self, engine: EngineType):

        if engine == EngineType.cromwell:
            return self.cromwell()

        # Returning none will allow the engine to run with no config
        return None


class LocalSingularityTemplate(SingularityEnvironmentTemplate):
    def __init__(
        self,
        executionDir=None,
        containerDir="/config/binaries/singularity/containers_devel/janis/",
        singularityLoadInstructions=None,
        containerBuildInstructions="singularity pull $image docker://${{docker}}",
        mailProgram: str = None,
    ):
        """

        :param executionDir: A location where the execution should take place
        :param containerDir: Location where to save and execute containers from
        :param singularityLoadInstructions: Ensure singularity with this command executed in shell
        :param containerBuildInstructions: Instructions for building singularity, it's recommended to not touch this setting.
        :param mailProgram: Mail program to pipe email to, eg: 'sendmail -t'
        """
        super().__init__(
            mail_program=mailProgram,
            containerDir=containerDir,
            buildInstructions=containerBuildInstructions,
            loadInstructions=singularityLoadInstructions,
        )

        self.executionDir = executionDir

    def cromwell(self):
        if not self.executionDir:
            return None

        config = CromwellConfiguration(
            backend=CromwellConfiguration.Backend(
                providers={
                    "Local": CromwellConfiguration.Backend.Provider.singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        buildinstructions=self.singularity_build_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        executionDirectory=self.executionDir,
                    )
                }
            )
        )

        return config

    def engine_config(self, engine: EngineType):

        if engine == EngineType.cromwell:
            return self.cromwell()

        elif engine == EngineType.cwltool:
            raise NotImplementedError(
                "Janis doesn't currently have a mechanism for configuring CWLTool for Singularity."
            )

        # Returning none will allow the engine to run with no config
        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
