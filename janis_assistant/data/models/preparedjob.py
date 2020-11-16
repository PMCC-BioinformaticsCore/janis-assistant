from typing import Dict, List, Union, Optional

from janis_core import Logger

from janis_assistant.containers import Container, get_container_by_name, ContainerType

from janis_assistant.validation import ValidationRequirements

from janis_assistant.management.configuration import (
    parse_if_dict,
    JanisConfigurationCromwell,
    JanisConfigurationTemplate,
    JanisConfigurationNotifications,
    JanisConfigurationEnvironment,
)

from janis_assistant.utils.batchrun import BatchRunRequirements

from janis_assistant.engines import EngineType

from janis_assistant.engines.cromwell.cromwellconfiguration import Serializable


class PreparedJob(Serializable):

    _instance = None  # type: PreparedJob

    @staticmethod
    def instance():
        """
        :return: JanisConfiguration
        """
        if PreparedJob._instance is None:
            raise Exception("Couldn't find instance of prepared submission")
        return PreparedJob._instance

    def __init__(
        self,
        config_dir: str = None,
        db_path: str = None,
        execution_dir=None,
        engine: str = None,
        cromwell: JanisConfigurationCromwell = None,
        template: JanisConfigurationTemplate = None,
        notifications: JanisConfigurationNotifications = None,
        environment: JanisConfigurationEnvironment = None,
        run_in_background: bool = None,
        digest_cache_location: str = None,
        # job information
        inputs: Dict = None,
        output_dir: str = None,
        keep_intermediate_files: bool = None,
        recipes: List[str] = None,
        hints: Dict[str, str] = None,
        allow_empty_container: bool = None,
        container_override: Dict[str, str] = None,
        skip_digest_lookup: bool = None,
        skip_digest_cache: bool = None,
        batchrun: Union[BatchRunRequirements, Dict] = None,
        store_in_central_db: bool = None,
        skip_file_check: bool = None,
        strict_inputs: bool = False,
        validation: ValidationRequirements = None,
        should_watch_if_background: bool = False,
        call_caching_enabled: bool = None,
        container_type: str = None,
        workflow_reference: str = None,
        post_run_script: str = None,
    ):
        """

        :param config_dir: The config_dir specifies where the janis.db, cromwell and workflow cache is.
        :param output_dir: This directory to copy outputs to. By default, the execution occurs in this directory (under <output>/janis) unless overriden with the '--exeution-dir' argument
        :param execution_dir: The directory which Janis meta and execution data is placed. If no execution directory is specified, it uses the path '<outputdir>/janis/'. Note that some templates may override the intermediate computation directory.
        :param engine:
        :param cromwell:
        :param template:
        :param notifications:
        :param run_in_background:
        :param digest_cache_location:
        :param inputs: YAML or JSON inputs file to provide values for the workflow (can specify multiple times)
        :param keep_intermediate_files: Do not remove execution directory on successful complete
        :param recipes:
        :param hints:
        :param allow_empty_container:
        :param container_override:
        :param skip_digest_lookup:
        :param skip_digest_cache:
        :param batchrun:
        :param store_in_central_db:
        :param skip_file_check:
        :param strict_inputs:
        :param validation:
        :param should_watch_if_background:
        :param cromwell_db_type:
        :param post_run_script:
        """
        self.config_dir = config_dir
        self.db_path = db_path

        self.output_dir = output_dir

        self.environment: JanisConfigurationEnvironment = parse_if_dict(
            JanisConfigurationEnvironment,
            environment or {},
            "environment",
            skip_if_empty=False,
        )

        self.engine = EngineType(engine) if engine else None

        requires_cromwell_config = self.engine == EngineType.cromwell
        self.cromwell: JanisConfigurationCromwell = parse_if_dict(
            JanisConfigurationCromwell,
            cromwell or {},
            "cromwell",
            skip_if_empty=not requires_cromwell_config,
        )
        self.template: JanisConfigurationTemplate = parse_if_dict(
            JanisConfigurationTemplate, template or {}, "template", skip_if_empty=False
        )
        self.notifications: JanisConfigurationNotifications = parse_if_dict(
            JanisConfigurationNotifications,
            notifications or {},
            "notifications",
            skip_if_empty=False,
        )
        self._workflow_reference = workflow_reference
        self.inputs = inputs
        self.hints = hints
        self.output_dir = output_dir
        self.execution_dir = execution_dir
        self.keep_intermediate_files = keep_intermediate_files
        self.recipes = recipes
        self.allow_empty_container = allow_empty_container
        self.container_override = container_override
        self.skip_digest_lookup = skip_digest_lookup
        self.skip_digest_cache = skip_digest_cache
        self.batchrun: Optional[BatchRunRequirements] = parse_if_dict(
            BatchRunRequirements, batchrun, "batchrun"
        )
        self.validation: Optional[ValidationRequirements] = parse_if_dict(
            ValidationRequirements, validation, "validation"
        )
        self.store_in_central_db = store_in_central_db
        self.skip_file_check = skip_file_check
        self.strict_inputs = strict_inputs
        self.should_watch_if_background = should_watch_if_background

        self.run_in_background = run_in_background
        self.digest_cache_location = digest_cache_location
        self.call_caching_enabled = call_caching_enabled

        self.post_run_script = post_run_script

        self.container_type = ContainerType(container_type)
        self._container = get_container_by_name(container_type)

        if not self._instance:
            Logger.debug("Setting prepared job")
            PreparedJob._instance = self
        else:
            Logger.debug("Setting prepared job, when already set")
            PreparedJob._instance = self

    def get_database_config_helper(self):
        if self.cromwell and self.cromwell.db_type:
            return self.cromwell.get_database_config_helper()

        return None
