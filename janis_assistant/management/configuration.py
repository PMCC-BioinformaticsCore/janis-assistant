import os.path
from copy import copy
from enum import Enum
from typing import Optional, List, Union, Type
import ruamel.yaml

from janis_assistant.containers import get_container_by_name
from janis_assistant.containers.base import Container

from janis_assistant.data.models.util import Serializable
from janis_core.utils.logger import Logger

from janis_assistant.utils import fully_qualify_filename

from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.data.enums.dbtype import DatabaseTypeToUse

from janis_assistant.management.envvariables import EnvVariables, HashableEnum
from janis_assistant.utils.callprogram import collect_output_from_command


def parse_if_dict(T: Type, val, path: str, skip_if_empty=True):
    if val is None:
        return None
    elif isinstance(val, T):
        return val
    elif isinstance(val, dict):
        if skip_if_empty and len(val) == 0:
            return None
        try:
            return T(**val)
        except TypeError as e:
            Logger.critical(
                f"Couldn't initialise key {path} of type '{T.__name__}', {str(e)}"
            )
            exit(1)
    raise Exception(
        f"Couldn't parse for type '{T.__name__}', "
        f"expected dict but received '{val.__class__.__name__}' for {str(val)}"
    )


class NoAttributeErrors:
    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            # Give None to support backwards compatible JanisConfigurations
            return None

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)


class JanisConfigurationTemplate(Serializable):
    def __init__(self, id: str = None, **d):
        """
        :param id:
        :type id: The identifier of the template
        """
        from janis_assistant.templates import from_template

        self.id = (
            id or EnvVariables.default_template.resolve() or "local"
        )  # change default here
        self.templateconfig = {k: v for k, v in d.items()}

        Logger.log("Got template ID: " + str(self.id))

        self.template = from_template(self.id, self.templateconfig)

    def to_dict(self):
        return {"id": self.id, **self.templateconfig}


class JanisConfigurationEnvironment(NoAttributeErrors, Serializable):
    def __init__(
        self, max_cores: int = None, max_memory: int = None, max_duration: int = None
    ):
        """
        Additional settings to configure a Janis environment. Currently, it mostly involves restricing resources (like cores, memory, duration) to fit within specific compute requirements.
        Notable, these values limit the requested values if they're a number. It doesn't currently limit this value if it's determined via an operator.

        :param max_cores: Limit the number of CPUs a job can request
        :type max_cores: int
        :param max_memory: Limit the amount of memory (in GB) a job can request
        :type max_memory: int
        :param max_duration: (Default: 86400) Limit the amount of time (in seconds) a job can request.
        :type max_duration: int
        """
        self.max_cores = max_cores
        self.max_memory = max_memory
        self.max_duration = max_duration


class MySqlInstanceConfig(Serializable):
    def __init__(self, url, username, password, dbname="cromwell"):
        """
        Configuration options for a MySQL instance

        :param url: URL of the mysql instance (including port if not 3036)
        :param username: Username
        :param password: Password, not this is embedded into the Cromwell configuration (<output-dir>/janis/configuration/cromwell.conf)
        :param dbname: Database name to use, default 'cromwell'
        """
        self.url = url
        self.username = username
        self.password = password
        self.dbname = dbname


class JanisConfigurationCromwell(Serializable):
    def __init__(
        self,
        jar: str = None,
        config_path: str = None,
        url: str = None,
        memory_mb: int = None,
        call_caching_method: str = "fingerprint",
        timeout: int = 10,
        polling_interval=None,
        db_type: DatabaseTypeToUse = DatabaseTypeToUse.filebased,
        mysql_credentials: Union[dict, MySqlInstanceConfig] = None,
        additional_config_lines: str = None,
    ):
        """
        :param url: Use an existing Cromwell instance with this URL (with port). Use the BASE url, do NOT include http.
        :type url: str
        :param jar: Specific Cromwell JAR to use (prioritised over ``$JANIS_CROMWELLJAR``)
        :param config_path: Use a supplied Config path when running a Cromwell instance. Also see ``additional_config_lines`` for including specific cromwell options.
        :param memory_mb: Amount of memory to give Cromwell instance through ``java -xmx <max-memory>M -jar <jar>``
        :param call_caching_method: (Default: "fingerprint") Cromwell caching strategy to use, see `Call cache strategy options for local filesystem <https://cromwell.readthedocs.io/en/stable/Configuring/#call-cache-strategy-options-for-local-filesystem>`_ for more information.
        :param timeout: Suspend a Janis workflow if unable to contact cromwell for <timeout> MINTUES.
        :param polling_interval: How often to poll Cromwell, by default this starts at 5 seconds, and gradually falls to 60 seconds over 30 minutes. For more information, see the ``janis_assistant.Cromwell.get_poll_interval`` `method <https://github.com/PMCC-BioinformaticsCore/janis-assistant/blob/master/janis_assistant/engines/cromwell/main.py#L179>`_
        :param db_type: (Default: filebased) DB type to use for Janis. "none" -> no database; "existing" -> use mysql credentials from ``cromwell.mysql_credentials``; "managed" -> Janis will start and manage a containerised MySQL instance; "filebased": Use the HSQLDB filebased db through Cromwell for SMALL workflows only (NB: this can produce large files, and timeout for large workflows); "from_script": Call the script ``$JANIS_DBCREDENTIALSGENERATOR`` for credentials. See `get_config_from_script <https://github.com/PMCC-BioinformaticsCore/janis-assistant/blob/master/janis_assistant/management/configuration.py#L621>`_ for more information.
        :type db_type: "none" | "existing" | "managed" | "filebased" | "from_script"
        :param mysql_credentials: A dictionary of MySQL credentials
        :type mysql_credentials: MySqlInstanceConfig
        :param additional_config_lines: A string to add to the bottom of a generated Cromwell configuration. This is NOT used for an existing cromwell instance, or a config is supplied.
        :type additional_config_lines: str
        """
        self.jar = jar
        self.config_path = config_path
        self.url = url
        self.memory_mb = memory_mb
        self.call_caching_method = call_caching_method
        self.timeout = timeout
        self.polling_interval = polling_interval
        self.db_type = (
            DatabaseTypeToUse(db_type) if db_type else DatabaseTypeToUse.filebased
        )
        self.mysql_credentials = None
        if mysql_credentials:
            self.mysql_credentials = parse_if_dict(
                MySqlInstanceConfig, mysql_credentials, "cromwell.mysql_credentials"
            )
        self.additional_params = additional_config_lines

    def get_database_config_helper(self):

        return JanisDatabaseConfigurationHelper(
            db_type=self.db_type, mysql_credentials=self.mysql_credentials
        )


class JanisConfigurationRecipes(Serializable):
    VALID_YAML_EXTENSIONS = {
        "yaml",
        "yml",
        "json",
    }  # json can be parsed by ruamel.yaml with no extra config

    def __init__(
        self,
        recipes: dict = None,
        paths: Union[str, List[str]] = None,
        directories: Union[str, List[str]] = None,
    ):
        """
        :param recipes: A dictionary of input values, keyed by the recipe name.
        :type recipes: dict
        :param paths: a list of ``*.yaml`` files, where each path contains a dictionary of input values, keyed by the recipe name, similar to the previous recipes name.
        :type paths: List[str]
        :param directories: a directory of ``*.yaml`` files, where the ``*`` is the recipe name.
        :type directories: List[str]
        """
        self.recipes = recipes or {}
        self.paths: Optional[List[str]] = None
        self.directories: Optional[List[str]] = None

        self._files_by_key = None
        self._loaded_recipes = False

        if paths:
            if isinstance(paths, list):
                self.paths = [fully_qualify_filename(d) for d in paths]
            else:
                self.paths = fully_qualify_filename(paths)

        if directories:
            if isinstance(directories, list):
                self.directories = [fully_qualify_filename(d) for d in directories]
            else:
                self.directories = fully_qualify_filename(directories)

    @staticmethod
    def parseable_yaml_filename_if_valid(path):
        base, ext = os.path.splitext(path)

        if len(ext) == 0:
            return os.path.basename(path), path

        if ext[1:] in JanisConfigurationRecipes.VALID_YAML_EXTENSIONS:
            return os.path.basename(path)[: -len(ext)], path

        return None

    def load_recipes(self, force=False):
        from os import listdir

        dirs: List[str] = []
        paths: List[str] = []

        paths_from_env = EnvVariables.recipe_paths.resolve(True)
        dirs_from_env = EnvVariables.recipe_directory.resolve(True) or []

        if paths_from_env:
            paths.extend(paths_from_env)
        if self.paths:
            paths.extend(self.paths)
        if dirs_from_env:
            dirs.extend(dirs_from_env)
        if self.directories:
            dirs.extend(self.directories)

        self._files_by_key = {}

        # Do if: force or (we haven't loaded recipes and we have recipes to load)
        if not (force or not self._loaded_recipes and (paths or dirs)):
            return

        import ruamel.yaml

        # Do the env first, then ones from the config can cascade over them

        for recipe_location in paths:
            try:
                with open(recipe_location) as rl:
                    adr = ruamel.yaml.load(rl, Loader=ruamel.yaml.Loader)
                    self.recipes.update(adr)

            except Exception as e:
                Logger.critical(f"Couldn't load recipe '{recipe_location}': {e}")

        for d in dirs:
            if not os.path.exists(d):
                Logger.critical(f"Couldn't find recipe directory: '{d}', skipping")
                continue
            if not os.path.isdir(d):
                Logger.critical(
                    f"The path listed as a recipe directory was not a directory: '{d}', skipping"
                )
                continue
            contents = listdir(d)
            for f in contents:
                fpath = os.path.join(d, f)
                parsed = self.parseable_yaml_filename_if_valid(fpath)
                if not parsed:
                    Logger.warn(
                        f"Skipping file within recipe directory '{fpath}' as it contained "
                        f"an unrecognised extension: '{os.path.splitext(fpath)[1]}"
                    )
                    continue

                key, value = parsed
                if key not in self._files_by_key:
                    self._files_by_key[key] = []
                self._files_by_key[key].append(value)

        self._loaded_recipes = True

    @staticmethod
    def get_cascaded_dict_from_yamls(files):
        d = {}
        for f in files:
            try:
                with open(f) as rl:
                    adr = ruamel.yaml.load(rl, Loader=ruamel.yaml.Loader)
                    d.update(adr)
            except Exception as e:
                Logger.critical(f"Couldn't parse file '{f}': {e}")
        return d

    def get_recipe_for_keys(self, keys: List[str]):

        self.load_recipes()

        if not keys:
            return {}

        rec = {}
        for key in keys:
            found_key = False

            if key in self._files_by_key:
                found_key = True
                rec.update(self.get_cascaded_dict_from_yamls(self._files_by_key[key]))

            if key in self.recipes:
                found_key = True
                rec.update(self.recipes[key] or {})

            if not found_key:
                Logger.critical(f"Couldn't find '{key}' in known recipes")

        return rec


class JanisConfigurationNotifications(Serializable):
    def __init__(
        self,
        email: str = None,
        from_email: str = "janis-noreply@petermac.org",
        mail_program: str = None,
    ):
        """
        :param email: Email address to send status updates to
        :param from_email: (Default: janis-noreply@petermac.org)
        :param mail_program: Which mail program to use to send emails. A fully formatted email will be directed as stdin (eg: sendmail -t)
        """

        self.email = email
        self.from_email = from_email
        self.mail_program = mail_program


class JanisConfiguration(NoAttributeErrors, Serializable):

    _configpath = None

    @staticmethod
    def initial_configuration(
        path: Optional[str], potential_paths: Optional[Union[str, List[str]]] = None
    ):

        paths_to_check = []
        if path is not None:
            if not os.path.exists(path):
                raise Exception(f"Couldn't find Janis configuration at path: {path}")
            paths_to_check = [path]
        else:
            if potential_paths:
                if isinstance(potential_paths, list):
                    paths_to_check.extend(potential_paths)
                else:
                    paths_to_check.append(potential_paths)

            default_path = EnvVariables.config_path.resolve(False)
            if default_path:
                paths_to_check.append(default_path)
            paths_to_check.append(EnvVariables.config_path.default())

        for p in paths_to_check:
            if p:
                p = os.path.expanduser(p)
            if not os.path.exists(p):
                continue

            with open(os.path.expanduser(p)) as cp:
                y = ruamel.yaml.safe_load(cp)
                return JanisConfiguration(**y)

        return JanisConfiguration()

    def __init__(
        self,
        output_dir: str = EnvVariables.output_dir.resolve(False),
        execution_dir: str = EnvVariables.exec_dir.resolve(False),
        call_caching_enabled: bool = True,
        engine: str = EngineType.cromwell.value,
        cromwell: Union[JanisConfigurationCromwell, dict] = None,
        template: Union[JanisConfigurationTemplate, dict] = None,
        recipes: Union[JanisConfigurationRecipes, dict] = None,
        notifications: Union[JanisConfigurationNotifications, dict] = None,
        environment: Union[JanisConfigurationEnvironment, dict] = None,
        run_in_background: bool = None,
        digest_cache_location: str = None,
        container: Union[str, Container] = None,
        search_paths: List[str] = None,
    ):
        """
        :param engine: Default engine to use
        :type engine: "cromwell" | "cwltool"
        :param template: Specify options for a Janis template for configuring an execution environment
        :type template: JanisConfigurationTemplate
        :param cromwell: A dictionary for how to configure Cromwell for Janis
        :type cromwell: JanisConfigurationCromwell
        :param recipes: Configure recipes in Janis
        :type recipes: JanisConfigurationRecipes
        :param notifications: Configure Janis notifications
        :type notifications: JanisConfigurationNotifications
        :param environment: Additional ways to configure the execution environment for Janis
        :type environment: JanisConfigurationEnvironment
        :param output_dir: A directory that Janis will use to generate a new output directory for each janis-run
        :param execution_dir: Move all execution to a static directory outside the regular output directory.
        :param call_caching_enabled: (default: true) call-caching is enabled for subsequent runs, on the SAME output directory
        :param run_in_background: By default, run workflows as a background process. In a SLURM environment, this might submit Janis as a SLURM job.
        :type run_in_background: bool
        :param digest_cache_location: A cache of docker tags to its digest that Janis uses replaces your docker tag with it's `digest <https://docs.docker.com/engine/reference/commandline/pull/#pull-an-image-by-digest-immutable-identifier>`_.
        :type digest_cache_location: str
        :param container: Container technology to use, important for checking if container environment is available and running mysql instance.
        :type container: "docker" | "singularity"
        :param search_paths: A list of paths to check when looking for python files and input files
        :type search_paths: List[str]
        """

        self.config_dir = EnvVariables.config_dir.resolve(True)
        self.db_path = fully_qualify_filename(os.path.join(self.config_dir, "janis.db"))
        self.digest_cache_location = digest_cache_location
        if not digest_cache_location:
            self.digest_cache_location = os.path.join(self.config_dir, "digest_cache")

        self.output_dir = output_dir
        self.execution_dir = execution_dir
        self.search_paths = search_paths or [os.path.expanduser("~/janis/")]

        self.engine = engine
        self.call_caching_enabled = call_caching_enabled
        self.run_in_background = run_in_background

        self.recipes = parse_if_dict(
            JanisConfigurationRecipes, recipes or {}, "recipes", skip_if_empty=False,
        )

        self.template = parse_if_dict(
            JanisConfigurationTemplate, template or {}, "template", skip_if_empty=False
        )
        self.cromwell: JanisConfigurationCromwell = parse_if_dict(
            JanisConfigurationCromwell, cromwell or {}, "cromwell", skip_if_empty=False
        )
        self.notifications: JanisConfigurationNotifications = parse_if_dict(
            JanisConfigurationNotifications,
            notifications or {},
            "notifications",
            skip_if_empty=False,
        )
        self.environment: JanisConfigurationEnvironment = parse_if_dict(
            JanisConfigurationEnvironment,
            environment or {},
            "environment",
            skip_if_empty=False,
        )

        # Get's set by the template for now, but eventually we should be able to look it up
        self.container = None
        if container:
            if isinstance(container, Container):
                self.container = container
            else:
                self.container = get_container_by_name(container)

        if self.template and self.template.template:
            self.template.template.post_configuration_hook(self)

    @staticmethod
    def get_value_for_key(d, key, default):
        val = d.get(key)
        if val is None:
            return default.get(key) if default else None

        Logger.log(f"Got value '{val}' for key '{key}'")
        return val

    @staticmethod
    def from_path(path: Optional[str]):
        p = path or EnvVariables.config_path.resolve(True)
        if p:
            p = os.path.expanduser(p)
        if p and os.path.exists(p):
            with open(os.path.expanduser(p)) as cp:
                y = ruamel.yaml.load(cp, Loader=ruamel.yaml.Loader)
                return JanisConfiguration(y)

        return JanisConfiguration()

    @staticmethod
    def default():
        """
        The defaults listed are provided to the user on init, they should be
        bareboned to the options that a user may often want to configure.

        These are not defaults during execution
        """
        deflt = {
            "engine": EngineType.cromwell.value,
            "notifications": {"email": None},
        }
        return stringify_dict_keys_or_return_value(deflt)


class JanisDatabaseConfigurationHelper(Serializable):
    def __init__(
        self,
        db_type: DatabaseTypeToUse,
        mysql_credentials: Optional[Union[MySqlInstanceConfig, dict]],
    ):
        self.db_type = db_type
        self.mysql_config = parse_if_dict(
            MySqlInstanceConfig, mysql_credentials, "mysql_config"
        )

    def which_db_to_use(self) -> DatabaseTypeToUse:
        return self.db_type

    def get_config_for_existing_config(self):
        t = self.which_db_to_use()
        if t != DatabaseTypeToUse.existing:
            raise Exception(
                f"Attempted to request database config for {DatabaseTypeToUse.existing.value} config, "
                f"but the database helper wants to use {t.value}"
            )
        from janis_assistant.engines.cromwell.cromwellconfiguration import (
            CromwellConfiguration,
        )

        config = self.mysql_config
        return CromwellConfiguration.Database.mysql(
            url=config.url,
            username=config.username,
            password=config.password,
            database=config.dbname,
            # maxConnections=2,
        )

    def get_config_for_filebased_db(self, path):
        t = self.which_db_to_use()
        if t != DatabaseTypeToUse.filebased:
            raise Exception(
                f"Attempted to request database config for {DatabaseTypeToUse.filebased.value} config, "
                f"but the database helper wants to use {t.value}"
            )
        from janis_assistant.engines.cromwell.cromwellconfiguration import (
            CromwellConfiguration,
        )

        return CromwellConfiguration.Database.filebased_db(location=path)

    def get_config_for_managed_mysql(self, url):
        t = self.which_db_to_use()
        if t != DatabaseTypeToUse.managed:
            raise Exception(
                f"Attempted to request database config for {DatabaseTypeToUse.managed.value} "
                f"config, but the database helper wants to use {t.value}"
            )

        from janis_assistant.engines.cromwell.cromwellconfiguration import (
            CromwellConfiguration,
        )

        return CromwellConfiguration.Database.mysql(
            username=None, password=None, url=url
        )

    def get_config_from_script(self, execution_dir: str):
        try:
            import subprocess, os, json
            from janis_assistant.management.envvariables import EnvVariables
            from janis_assistant.engines.cromwell.cromwellconfiguration import (
                CromwellConfiguration,
            )

            file_path = os.getenv(EnvVariables.db_script_generator)
            Logger.debug(
                f"Found path '{EnvVariables.db_script_generator}' to generate database credentials"
            )
            if file_path is None:
                raise Exception(
                    f"Couldn't get database credentials as couldn't find value in env var '{EnvVariables.db_script_generator}'"
                )
            # if not os.path.exists(file_path):
            #     raise Exception(f"Couldn't locate script '{file_path}' to execute")

            try:
                val = collect_output_from_command(
                    f"{file_path} {execution_dir}", stderr=Logger.guess_log, shell=True
                )
            except Exception as e:
                Logger.critical(f"Failed to generate database credentials ({repr(e)})")
                raise
            d = json.loads(val)
            Logger.debug(
                "Received keys from database credentials script: " + ", ".join(d.keys())
            )

            keys = {"username", "password", "database", "host"}
            missing_keys = {k for k in keys if k not in d}
            if len(missing_keys) > 0:
                raise Exception(
                    "The script to generate database credentials was missing the keys: "
                    + ", ".join(missing_keys)
                )

            return CromwellConfiguration.Database.mysql(
                username=d["username"],
                password=d["password"],
                database=d["database"],
                url=d["host"],
            )
        except Exception as e:
            Logger.critical(
                "Failed to get database configuration details from script: " + repr(e)
            )
            raise

    def run_delete_database_script(self, execution_dir: str):
        try:
            import subprocess, os
            from janis_assistant.management.envvariables import EnvVariables

            file_path = os.getenv(EnvVariables.db_script_generator_cleanup)

            if file_path is None:
                raise Exception(
                    f"Couldn't delete generated database credentials as couldn't find value in env var '{EnvVariables.db_script_generator_cleanup}'"
                )
            Logger.debug(
                f"Found path '{EnvVariables.db_script_generator_cleanup}' to delete database credentials"
            )
            # if not os.path.exists(file_path):
            #     raise Exception(f"Couldn't locate script '{file_path}' to execute")

            val = collect_output_from_command(
                f"{file_path} {execution_dir}", stderr=Logger.guess_log, shell=True
            )
            if val is not None and len(val) > 0:
                Logger.info(
                    f"Successfully deleted DB credentials and received message: {val}"
                )
            else:
                Logger.info("Deleted credentials with rc=0")
        except Exception as e:
            Logger.warn(
                f"Failed to delete database configuration details for execution directory '{execution_dir}': "
                + repr(e)
            )


def stringify_dict_keys_or_return_value(d):
    if d is None:
        return d
    if isinstance(d, list):
        return [stringify_dict_keys_or_return_value(dd) for dd in d]
    if isinstance(d, int) or isinstance(d, float) or isinstance(d, bool):
        return d
    if not isinstance(d, dict):
        return str(d)

    out = {}
    for k, v in d.items():
        out[str(k)] = stringify_dict_keys_or_return_value(v)
    return out
