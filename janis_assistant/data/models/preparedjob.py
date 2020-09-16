from typing import Dict, List, Union, Optional

from janis_assistant.validation import ValidationRequirements

from janis_assistant.management.configuration import (
    parse_if_dict,
    JanisConfiguration,
    JanisDatabaseConfigurationHelper,
    DatabaseTypeToUse,
)

from janis_assistant.utils.batchrun import BatchRunRequirements

from janis_assistant.engines import EngineType

from janis_assistant.engines.cromwell.cromwellconfiguration import Serializable


class PreparedSubmission(Serializable):
    input_categories = [
        (None, ["inputs", "output_dir", "execution_dir", "foreground"]),
        ("input manipulation", ["max_cores", "max_memory", "max_duration"]),
        ("container related args", ["allow_empty_container", "container_override"]),
    ]
    input_aliases = {
        "inputs": "i",
        "output_dir": "o",
        "execution_dir": "E",
        "background": "B",
        "foreground": "F",
    }

    def __init__(
        self,
        config: Union[JanisConfiguration, dict],
        inputs: Dict = None,
        output_dir: str = None,
        execution_dir: str = None,
        keep_intermediate_files: bool = None,
        development: bool = None,
        recipes: List[str] = None,
        hints: Dict[str, str] = None,
        max_cores: int = None,
        max_memory: int = None,
        max_duration: int = None,
        allow_empty_container: bool = None,
        container_override: Dict[str, str] = None,
        skip_digest_lookup: bool = None,
        skip_digest_cache: bool = None,
        engine: EngineType = None,
        batchrun: Union[BatchRunRequirements, Dict] = None,
        no_store: bool = None,
        skip_file_check: bool = None,
        strict_inputs: bool = False,
        foreground: bool = False,
        background: bool = False,
        validation: ValidationRequirements = None,
        should_watch_if_background: bool = False,
        cromwell_db_type: DatabaseTypeToUse = None,
    ):
        """
        :param inputs: YAML or JSON inputs file to provide values for the workflow (can specify multiple times)
        :param output_dir: This directory to copy outputs to. By default, the execution occurs in this directory (under <output>/janis) unless overriden with the '--exeution-dir' argument
        :param execution_dir: The directory which Janis meta and execution data is placed. If no execution directory is specified, it uses the path '<outputdir>/janis/'. Note that some templates may override the intermediate computation directory.
        :param keep_intermediate_files: Do not remove execution directory on successful complete
        :param development: Apply common settings (--keep-execution-dir) to support incremental development of a pipeline
        :param recipes:
        :param hints:
        :param max_cores:
        :param max_memory:
        :param max_duration:
        :param allow_empty_container:
        :param container_override:
        :param skip_digest_lookup:
        :param skip_digest_cache:
        :param engine:
        :param batchrun:
        :param no_store:
        :param skip_file_check:
        :param foreground: Run workflow engine in foreground
        :param background: Run the workflow engine in the background (or submit to a cluster if your template supports it)
        """
        self.config: JanisConfiguration = parse_if_dict(
            JanisConfiguration, config, "config"
        )
        if not self.config:
            raise Exception("Expected valid 'config' when creating job")
        self.inputs = inputs
        self.hints = hints
        self.output_dir = output_dir
        self.execution_dir = execution_dir
        self.keep_intermediate_files = keep_intermediate_files
        self.development = development
        self.recipes = recipes
        self.max_cores = max_cores
        self.max_memory = max_memory
        self.max_duration = max_duration
        self.allow_empty_container = allow_empty_container
        self.container_override = container_override
        self.skip_digest_lookup = skip_digest_lookup
        self.skip_digest_cache = skip_digest_cache
        self.engine = EngineType(engine) if engine else None
        self.batchrun: Optional[BatchRunRequirements] = parse_if_dict(
            BatchRunRequirements, batchrun, "batchrun"
        )
        self.validation: Optional[ValidationRequirements] = parse_if_dict(
            ValidationRequirements, validation, "validation"
        )
        self.no_store = no_store
        self.skip_file_check = skip_file_check
        self.strict_inputs = strict_inputs
        self.foreground = foreground
        self.background = background
        self.should_watch_if_background = should_watch_if_background

        self.cromwell_db_type = cromwell_db_type

        # more thought required to turn this into a configurable parameter
        self.dry_run = False

    def get_database_config_helper(self):
        if self.cromwell_db_type is not None:
            return JanisDatabaseConfigurationHelper(
                db_type=self.cromwell_db_type, mysql_credentials=None
            )

        elif self.config.cromwell:
            return self.config.cromwell.get_database_config_helper()
        return None
