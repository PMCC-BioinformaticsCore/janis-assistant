import os.path
from enum import Enum
from typing import Optional, List, Union
import ruamel.yaml
from janis_core.utils.logger import Logger

from janis_assistant.utils import fully_qualify_filename

from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.engines.cromwell.cromwellconfiguration import DatabaseTypeToUse

from janis_assistant.management.envvariables import EnvVariables, HashableEnum
from janis_assistant.templates import from_template
from janis_assistant.utils.callprogram import collect_output_from_command


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


class JanisConfiguration(NoAttributeErrors):
    class Keys(HashableEnum):
        ConfigDir = "config_dir"
        OutputDir = "output_dir"
        ExecutionDir = "execution_dir"
        SearchPaths = "search_paths"
        Engine = "engine"
        Environment = "environment"
        Cromwell = "cromwell"
        Template = "template"
        Recipes = "recipes"
        Notifications = "notifications"
        RunInBackground = "run_in_background"
        CallCachingEnabled = "call_caching_enabled"
        DigestCacheLocation = "digest_cache_location"

    _managed = None  # type: JanisConfiguration

    _configpath = None

    @staticmethod
    def manager():
        """
        :return: JanisConfiguration
        """
        if not JanisConfiguration._managed:
            JanisConfiguration._managed = JanisConfiguration()
        return JanisConfiguration._managed

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
                JanisConfiguration._managed = JanisConfiguration(y)
                break

        if not JanisConfiguration._managed:
            JanisConfiguration._managed = JanisConfiguration()

        return JanisConfiguration._managed

    class JanisConfigurationTemplate(NoAttributeErrors):
        class Keys(HashableEnum):
            Id = "id"

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.id = JanisConfiguration.get_value_for_key(d, self.Keys.Id, default)

            Logger.log("Got template ID: " + str(self.id))

            # remove this id from the dictionary: https://stackoverflow.com/a/15411146/
            d.pop(self.Keys.Id.value, None)
            self.template = from_template(self.id, d)

    class JanisConfigurationEnvironment(NoAttributeErrors):
        class Keys(HashableEnum):
            MaxCores = "max_cores"
            MaxRam = "max_ram"
            MaxDuration = "max_duration"

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.max_cores = JanisConfiguration.get_value_for_key(
                d, self.Keys.MaxCores, default
            )
            self.max_ram = JanisConfiguration.get_value_for_key(
                d, self.Keys.MaxRam, default
            )
            self.max_duration = JanisConfiguration.get_value_for_key(
                d, self.Keys.MaxDuration, default
            )

    class JanisConfigurationCromwell(NoAttributeErrors):
        class Keys(HashableEnum):
            JarPath = "jar"
            ConfigPath = "config_path"
            Url = "url"
            Memory = "memory_mb"
            CallCachingMethod = "call_caching_method"
            Timeout = "timeout"
            MySqlURL = "mysql_url"
            MySqlUsername = "mysql_username"
            MySqlPassword = "mysql_password"
            MySqlDbName = "mysql_dbname"
            UseManagedMySqlInstance = "should_manage_mysql"
            UseDatabase = "use_database"
            GeneratedDbCredentials = "generated_db_credentials_from_script"
            AdditionalParameters = "additional_params"  # string or list of strings
            PollingInterval = "polling_interval"

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.jarpath = fully_qualify_filename(
                JanisConfiguration.get_value_for_key(d, self.Keys.JarPath, default)
            )
            self.configpath = JanisConfiguration.get_value_for_key(
                d, self.Keys.ConfigPath, default
            )

            self.url = JanisConfiguration.get_value_for_key(d, self.Keys.Url, default)
            self.memory = JanisConfiguration.get_value_for_key(
                d, self.Keys.Memory, default
            )

            self.polling_interval = JanisConfiguration.get_value_for_key(
                d, self.Keys.PollingInterval, default
            )

            self.call_caching_method = JanisConfiguration.get_value_for_key(
                d, self.Keys.CallCachingMethod, default
            )

            self.timeout = JanisConfiguration.get_value_for_key(
                d, self.Keys.Timeout, default
            )
            self.mysql_url = JanisConfiguration.get_value_for_key(
                d, self.Keys.MySqlURL, default
            )
            self.mysql_username = JanisConfiguration.get_value_for_key(
                d, self.Keys.MySqlUsername, default
            )
            self.mysql_password = JanisConfiguration.get_value_for_key(
                d, self.Keys.MySqlPassword, default
            )
            self.mysql_dbname = JanisConfiguration.get_value_for_key(
                d, self.Keys.MySqlDbName, default
            )
            self.mysql_instance = JanisConfiguration.get_value_for_key(
                d, self.Keys.UseManagedMySqlInstance, default
            )

            self.use_database = JanisConfiguration.get_value_for_key(
                d, self.Keys.UseDatabase, default
            )
            self.generated_db_credentials_from_script = (
                JanisConfiguration.get_value_for_key(
                    d, self.Keys.GeneratedDbCredentials, default
                )
            )

            self.additional_params = JanisConfiguration.get_value_for_key(
                d, self.Keys.AdditionalParameters, default
            )

        def get_database_config_helper(self):
            existing_config = None
            if self.mysql_url:
                existing_config = JanisDatabaseConfigurationHelper.MySqlInstanceConfig(
                    url=self.mysql_url,
                    username=self.mysql_username,
                    password=self.mysql_password,
                    dbname=self.mysql_dbname,
                )

            return JanisDatabaseConfigurationHelper(
                mysql_config=existing_config,
                skip_database=self.use_database is False,
                run_managed_mysql_instance=self.mysql_instance,
                generated_db_credentials_from_script=self.generated_db_credentials_from_script,
            )

    class JanisConfigurationRecipes(NoAttributeErrors):
        class Keys(HashableEnum):
            Recipes = "recipes"
            Paths = "paths"
            Directories = "directories"

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.recipes = (
                JanisConfiguration.get_value_for_key(d, self.Keys.Recipes, default)
                or {}
            )
            rps = JanisConfiguration.get_value_for_key(d, self.Keys.Paths, default)
            self.recipe_paths = []
            if rps:
                self.recipe_paths = rps if isinstance(rps, list) else [rps]

            dirs = JanisConfiguration.get_value_for_key(
                d, self.Keys.Directories, default
            )

            if dirs:
                if isinstance(dirs, list):
                    dirs = [fully_qualify_filename(d) for d in dirs]
                else:
                    dirs = fully_qualify_filename(dirs)

            self.recipe_directories = dirs

            self._files_by_key = None
            self._loaded_recipes = False

        VALID_YAML_EXTENSIONS = {
            "yaml",
            "yml",
            "json",
        }  # json can be parsed by ruamel.yaml with no extra config

        @staticmethod
        def parseable_yaml_filename_if_valid(path):
            base, ext = os.path.splitext(path)

            if len(ext) == 0:
                return os.path.basename(path), path

            if (
                ext[1:]
                in JanisConfiguration.JanisConfigurationRecipes.VALID_YAML_EXTENSIONS
            ):
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
            if self.recipe_paths:
                paths.extend(self.recipe_paths)
            if dirs_from_env:
                dirs.extend(dirs_from_env)
            if self.recipe_directories:
                dirs.extend(self.recipe_directories)

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
                    rec.update(
                        self.get_cascaded_dict_from_yamls(self._files_by_key[key])
                    )

                if key in self.recipes:
                    found_key = True
                    rec.update(self.recipes[key] or {})

                if not found_key:
                    Logger.critical(f"Couldn't find '{key}' in known recipes")

            return rec

    class JanisConfigurationNotifications:
        class Keys(HashableEnum):
            # Events = "events"   # All events currently
            Email = "email"
            FromEmail = "fromEmail"
            MailProgram = "mail_program"
            # Slack = "slack"     # unused

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.email = JanisConfiguration.get_value_for_key(
                d, self.Keys.Email, default
            )
            self.from_email = JanisConfiguration.get_value_for_key(
                d, self.Keys.FromEmail, default
            )

            self.mail_program = JanisConfiguration.get_value_for_key(
                d, self.Keys.MailProgram, default
            )

    def __init__(self, d: dict = None):
        default = self.base()
        d = d if d else {}

        extra = "" if d is None else " from loaded config"
        Logger.debug("Instantiating JanisConfiguration" + extra)

        self.configdir = fully_qualify_filename(
            self.get_value_for_key(d, JanisConfiguration.Keys.ConfigDir, default)
        )
        self.dbpath = fully_qualify_filename(os.path.join(self.configdir, "janis.db"))
        self.outputdir = fully_qualify_filename(
            self.get_value_for_key(d, JanisConfiguration.Keys.OutputDir, default)
        )

        self.executiondir = fully_qualify_filename(
            self.get_value_for_key(d, JanisConfiguration.Keys.ExecutionDir, default)
        )
        self.call_caching_enabled = JanisConfiguration.get_value_for_key(
            d, self.Keys.CallCachingEnabled, default
        )

        self.engine = self.get_value_for_key(d, JanisConfiguration.Keys.Engine, default)
        self.cromwell = JanisConfiguration.JanisConfigurationCromwell(
            d.get(JanisConfiguration.Keys.Cromwell),
            default.get(JanisConfiguration.Keys.Cromwell),
        )

        self.template: JanisConfiguration.JanisConfigurationTemplate = (
            JanisConfiguration.JanisConfigurationTemplate(
                d.get(JanisConfiguration.Keys.Template),
                default.get(JanisConfiguration.Keys.Template),
            )
        )

        self.recipes = JanisConfiguration.JanisConfigurationRecipes(
            d.get(JanisConfiguration.Keys.Recipes),
            default.get(JanisConfiguration.Keys.Recipes),
        )

        self.notifications = JanisConfiguration.JanisConfigurationNotifications(
            d.get(JanisConfiguration.Keys.Notifications),
            default.get(JanisConfiguration.Keys.Notifications),
        )

        self.environment = JanisConfiguration.JanisConfigurationEnvironment(
            d.get(JanisConfiguration.Keys.Environment),
            default.get(JanisConfiguration.Keys.Environment),
        )

        self.run_in_background = self.get_value_for_key(
            d, JanisConfiguration.Keys.RunInBackground, default
        )
        self.digest_cache_location = fully_qualify_filename(
            self.get_value_for_key(d, JanisConfiguration.Keys.DigestCacheLocation, {})
        )
        if not self.digest_cache_location:
            self.digest_cache_location = os.path.join(self.configdir, "digest_cache")

        sp = self.get_value_for_key(d, JanisConfiguration.Keys.SearchPaths, default)
        self.searchpaths = sp if isinstance(sp, list) else [sp]
        env_sp = EnvVariables.search_path.resolve(False)
        if env_sp and env_sp not in self.searchpaths:
            self.searchpaths.append(fully_qualify_filename(env_sp))

        # Get's set by the template for now, but eventually we should be able to look it up
        self.container = None

        JanisConfiguration._managed = self

        if self.template.template:
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
    def base():
        """
        The defaults listed here should be sensible defaults

        :return:
        """

        deflt = {
            JanisConfiguration.Keys.ConfigDir: EnvVariables.config_dir.resolve(True),
            JanisConfiguration.Keys.OutputDir: EnvVariables.output_dir.resolve(False),
            JanisConfiguration.Keys.ExecutionDir: EnvVariables.exec_dir.resolve(False),
            JanisConfiguration.Keys.SearchPaths: [os.path.expanduser("~/janis/")],
            JanisConfiguration.Keys.Engine: EngineType.cromwell.value,
            JanisConfiguration.Keys.Cromwell: {
                # Resolved at runtime using "ConfigDir + cromwell-*.jar" else None, and then it's downloaded
                JanisConfiguration.JanisConfigurationCromwell.Keys.JarPath: None,
                JanisConfiguration.JanisConfigurationCromwell.Keys.Timeout: 10,
                JanisConfiguration.JanisConfigurationCromwell.Keys.CallCachingMethod: "fingerprint",
            },
            JanisConfiguration.Keys.Environment: {},
            JanisConfiguration.Keys.Template: {
                JanisConfiguration.JanisConfigurationTemplate.Keys.Id: "local"
            },
            JanisConfiguration.Keys.Notifications: {
                JanisConfiguration.JanisConfigurationNotifications.Keys.Email: None,
                JanisConfiguration.JanisConfigurationNotifications.Keys.FromEmail: "janis-noreply@petermac.org",
            },
            JanisConfiguration.Keys.RunInBackground: None,
            JanisConfiguration.Keys.CallCachingEnabled: True,
        }
        return stringify_dict_keys_or_return_value(deflt)

    @staticmethod
    def default():
        """
        The defaults listed are provided to the user on init, they should be
        bareboned to the options that a user may often want to configure.

        These are not defaults during execution
        """
        deflt = {
            JanisConfiguration.Keys.Engine: EngineType.cromwell.value,
            JanisConfiguration.Keys.Notifications: {
                JanisConfiguration.JanisConfigurationNotifications.Keys.Email: None
            },
        }
        return stringify_dict_keys_or_return_value(deflt)


class JanisDatabaseConfigurationHelper:
    class MySqlInstanceConfig:
        def __init__(self, url, username, password, dbname="cromwell"):
            self.url = url
            self.username = username
            self.password = password
            self.dbname = dbname

    def __init__(
        self,
        mysql_config: MySqlInstanceConfig = None,
        run_managed_mysql_instance=None,
        skip_database=None,
        generated_db_credentials_from_script=None,
    ):
        self.mysql_config = mysql_config
        self.should_manage_mysql = run_managed_mysql_instance
        self.skip_database = skip_database
        self.generated_db_credentials_from_script = generated_db_credentials_from_script

    def which_db_to_use(self) -> DatabaseTypeToUse:
        if self.mysql_config is not None:
            return DatabaseTypeToUse.existing
        elif self.skip_database:
            return DatabaseTypeToUse.none
        elif self.should_manage_mysql is True:
            return DatabaseTypeToUse.managed
        elif self.generated_db_credentials_from_script is True:
            return DatabaseTypeToUse.from_script
        return DatabaseTypeToUse.filebased

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

    def get_config_for_template_supplied(self, execution_dir: str):
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
