import os
from enum import Enum
from typing import Optional
import ruamel.yaml

from janis_runner.utils import Logger


class HashableEnum(str, Enum):
    pass
    # def __hash__(self):
    #     return self.value.__hash__()


class EnvVariables(HashableEnum):
    config_path = "JANIS_CONFIGPATH"
    config_dir = "JANIS_CONFIGDIR"
    exec_dir = "JANIS_EXCECUTIONDIR"
    search_path = "JANIS_SEARCHPATH"

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

        raise Exception(f"Couldn't determine default() for '{self.value}'")

    def resolve(self, include_default=False):
        return os.getenv(self.value, self.default() if include_default else None)


class JanisConfiguration:
    class Keys(HashableEnum):
        ConfigDir = "configDir"
        ExecutionDir = "executionDir"
        SearchPaths = "searchPaths"
        Environment = "environment"

    class JanisConfigurationEnvironment:
        class Keys(HashableEnum):
            Default = "default"

        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.default = JanisConfiguration.get_value_for_key(d, self.Keys.Default, default)

    def __init__(self, d: dict = None):
        default = self.default()
        d = d if d else {}

        self.configdir = self.get_value_for_key(d, JanisConfiguration.Keys.ConfigDir, default)
        self.dbpath = os.path.join(self.configdir, "janis.db")
        self.executiondir = self.get_value_for_key(d, JanisConfiguration.Keys.ExecutionDir, default)

        self.environment = JanisConfiguration.JanisConfigurationEnvironment(
            d.get(JanisConfiguration.Keys.Environment),
            default[JanisConfiguration.Keys.Environment],
        )

        sp = self.get_value_for_key(d, JanisConfiguration.Keys.SearchPaths, default)
        self.searchpaths = sp if isinstance(sp, list) else [sp]
        env_sp = EnvVariables.search_path.resolve(False)
        if env_sp and env_sp not in self.searchpaths:
            self.searchpaths.append(env_sp)

    @staticmethod
    def get_value_for_key(d, key, default):
        val = d.get(key)
        if not val:
            return default[key]

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

        return {
            JanisConfiguration.Keys.ConfigDir: EnvVariables.config_dir.resolve(True),
            JanisConfiguration.Keys.ExecutionDir: EnvVariables.exec_dir.resolve(True),
            JanisConfiguration.Keys.SearchPaths: [os.path.expanduser("~/janis/")],
            JanisConfiguration.Keys.Environment: {
                JanisConfiguration.JanisConfigurationEnvironment.Keys.Default: None
            },
        }
