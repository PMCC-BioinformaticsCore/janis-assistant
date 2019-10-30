import os
from enum import Enum
from typing import Optional, List, Union
import ruamel.yaml

from janis_runner.engines.enginetypes import EngineType
from janis_core.utils.logger import Logger

from janis_runner.templates import from_template


class HashableEnum(str, Enum):
    def __str__(self):
        return self.value

    def to_yaml(self):
        return self.value

    pass
    # def __hash__(self):
    #     return self.value.__hash__()


class EnvVariables(HashableEnum):
    config_path = "JANIS_CONFIGPATH"
    config_dir = "JANIS_CONFIGDIR"
    exec_dir = "JANIS_EXCECUTIONDIR"
    search_path = "JANIS_SEARCHPATH"
    recipe_paths = "JANIS_RECIPEPATHS"
    recipe_directory = "JANIS_RECIPEDIRECTORY"  # secretly comma separated

    cromwelljar = "JANIS_CROMWELLJAR"

    def __str__(self):
        return self.value

    def default(self):
        import os

        if self == EnvVariables.config_dir:
            return os.path.join(os.getenv("HOME"), ".janis/")
        elif self == EnvVariables.exec_dir:
            return os.path.join(os.getenv("HOME"), "janis/execution/")
        elif self == EnvVariables.config_path:
            return os.path.join(os.getenv("HOME"), ".janis/janis.conf")
        elif self == EnvVariables.recipe_paths:
            return []
        elif self == EnvVariables.recipe_directory:
            return []

        raise Exception(f"Couldn't determine default() for '{self.value}'")

    def resolve(self, include_default=False):
        value = os.getenv(self.value, self.default() if include_default else None)
        if self == EnvVariables.recipe_paths:
            return value.split(",") if value else None
        if self == EnvVariables.recipe_directory:
            return value.split(",") if value else None
        return value


class JanisConfiguration:
    class Keys(HashableEnum):
        ConfigDir = "configDir"
        OutputDir = "outputDir"
        SearchPaths = "searchPaths"
        Engine = "engine"
        Environment = "environment"
        Cromwell = "cromwell"
        Template = "template"
        Recipes = "recipes"
        Notifications = "notifications"

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
    def initial_configuration(potential_paths: Optional[Union[str, List[str]]]):

        paths_to_check = []
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

    class JanisConfigurationTemplate:
        class Keys(HashableEnum):
            Id = "id"

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.id = JanisConfiguration.get_value_for_key(d, self.Keys.Id, default)

            Logger.log("Got template ID: " + str(self.id))

            # remove this id from the dictionary: https://stackoverflow.com/a/15411146/
            d.pop(self.Keys.Id.value, None)
            self.template = from_template(self.id, d)

    class JanisConfigurationCromwell:
        class Keys(HashableEnum):
            JarPath = "jar"
            ConfigPath = "configPath"

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.jarpath = JanisConfiguration.get_value_for_key(
                d, self.Keys.JarPath, default
            )
            self.configpath = JanisConfiguration.get_value_for_key(
                d, self.Keys.ConfigPath, default
            )

    class JanisConfigurationRecipes:
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
            self.recipe_paths = rps if isinstance(rps, list) else [rps]

            dirs = JanisConfiguration.get_value_for_key(
                d, self.Keys.Directories, default
            )
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
            if not force and (self._loaded_recipes or not self.recipe_paths):
                return

            import ruamel.yaml

            paths = []

            # Do the env first, then ones from the config can cascade over them
            paths_from_env = EnvVariables.recipe_paths.resolve(True)
            if paths_from_env:
                paths.extend(paths_from_env)

            if self.recipe_paths:
                paths.extend(self.recipe_paths)

            for recipe_location in paths:
                try:
                    with open(recipe_location) as rl:
                        adr = ruamel.yaml.load(rl, Loader=ruamel.yaml.Loader)
                        self.recipes.update(adr)

                except Exception as e:
                    Logger.critical(f"Couldn't load recipe '{recipe_location}': {e}")

            dirs: List[str] = []
            dirs.extend(EnvVariables.recipe_directory.resolve(True) or [])
            dirs.extend(self.recipe_directories or [])

            self._files_by_key = {}
            for d in dirs:
                if not os.path.exists(d):
                    Logger.critical(f"Couldn't find recipe directory: '{d}', skipping")
                    continue
                if not os.path.isdir(d):
                    Logger.critical(
                        f"The path listed as a recipe directory was not a directory: '{d}', skipping"
                    )
                    continue
                contents = os.listdir(d)
                for f in contents:
                    fpath = os.path.join(d, f)
                    parsed = self.parseable_yaml_filename_if_valid(fpath)
                    if not parsed:
                        Logger.log(
                            f"Skipping file within recipe directory '{fpath}' as it contained "
                            f"an unrecognised extension: '{os.path.splitext(fpath)[1]}"
                        )

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
            MailProgram = "mailProgram"
            # Slack = "slack"     # unused

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.email = JanisConfiguration.get_value_for_key(
                d, self.Keys.Email, default
            )

            self.mail_program = JanisConfiguration.get_value_for_key(
                d, self.Keys.MailProgram, default
            )

    def __init__(self, d: dict = None):
        default = self.base()
        d = d if d else {}

        extra = "" if d is None else " from loaded config"
        Logger.log("Instantiating JanisConfiguration" + extra)

        self.configdir = self.get_value_for_key(
            d, JanisConfiguration.Keys.ConfigDir, default
        )
        self.dbpath = os.path.join(self.configdir, "janis.db")
        self.outputdir = self.get_value_for_key(
            d, JanisConfiguration.Keys.OutputDir, default
        )

        self.engine = self.get_value_for_key(d, JanisConfiguration.Keys.Engine, default)
        self.cromwell = JanisConfiguration.JanisConfigurationCromwell(
            d.get(JanisConfiguration.Keys.Cromwell),
            default.get(JanisConfiguration.Keys.Cromwell),
        )

        self.template = JanisConfiguration.JanisConfigurationTemplate(
            d.get(JanisConfiguration.Keys.Template),
            default.get(JanisConfiguration.Keys.Template),
        )

        self.recipes = JanisConfiguration.JanisConfigurationRecipes(
            d.get(JanisConfiguration.Keys.Recipes),
            default.get(JanisConfiguration.Keys.Recipes),
        )

        self.notifications = JanisConfiguration.JanisConfigurationNotifications(
            d.get(JanisConfiguration.Keys.Notifications),
            default.get(JanisConfiguration.Keys.Notifications),
        )

        sp = self.get_value_for_key(d, JanisConfiguration.Keys.SearchPaths, default)
        self.searchpaths = sp if isinstance(sp, list) else [sp]
        env_sp = EnvVariables.search_path.resolve(False)
        if env_sp and env_sp not in self.searchpaths:
            self.searchpaths.append(env_sp)

        # Get's set by the template for now, but eventually we should be able to look it up
        self.container = None

        JanisConfiguration._managed = self

        if self.template.template:
            self.template.template.post_configuration_hook(self)

    @staticmethod
    def get_value_for_key(d, key, default):
        val = d.get(key)
        if not val:
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
            JanisConfiguration.Keys.OutputDir: EnvVariables.exec_dir.resolve(True),
            JanisConfiguration.Keys.SearchPaths: [os.path.expanduser("~/janis/")],
            JanisConfiguration.Keys.Engine: EngineType.cromwell.value,
            # JanisConfiguration.Keys.Cromwell: {
            #     # Resolved at runtime using "ConfigDir + cromwell-*.jar" else None, and then it's downloaded
            #     JanisConfiguration.JanisConfigurationCromwell.Keys.JarPath: None,
            # },
            JanisConfiguration.Keys.Template: {
                JanisConfiguration.JanisConfigurationTemplate.Keys.Id: "local"
            },
            JanisConfiguration.Keys.Notifications: {
                JanisConfiguration.JanisConfigurationNotifications.Keys.Email: None
            },
        }
        return stringify_dict_keys_or_return_value(deflt)

    @staticmethod
    def default():
        """
        The defaults listed are provided to the user on init, they should be
        bareboned to the options that a user may often want to configure.
        """
        deflt = {
            JanisConfiguration.Keys.OutputDir: EnvVariables.exec_dir.resolve(True),
            JanisConfiguration.Keys.Engine: EngineType.cromwell.value,
            JanisConfiguration.Keys.Notifications: {
                JanisConfiguration.JanisConfigurationNotifications.Keys.Email: None
            },
        }
        return stringify_dict_keys_or_return_value(deflt)


def stringify_dict_keys_or_return_value(d):
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
