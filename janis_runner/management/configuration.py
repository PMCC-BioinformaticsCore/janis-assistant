import os
from enum import Enum
from typing import Optional
import ruamel.yaml


class EnvVariables(Enum):
    config_path = "JANIS_CONFIGPATH"
    config_dir = "JANIS_CONFIGDIR"
    exec_dir = "JANIS_OUTPUTDIR"
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
    class JanisConfigurationEnvironment:
        def __init__(self, d: dict, default: dict):
            d = d if d else {}

            self.default = d.get("default", default["default"])

    def __init__(self, d: dict = None):
        default = self.default()
        d = d if d else {}

        self.configdir = d.get("configdir", default["configdir"])
        self.dbpath = os.path.join(self.configdir, "janis.db")
        self.outputdir = d.get("outputdir", default["outputdir"])

        self.environment = JanisConfiguration.JanisConfigurationEnvironment(
            d.get("environment"), default["environment"]
        )

        sp = d.get("searchpaths", default["searchpaths"])
        self.searchpaths = sp if isinstance(sp, list) else [sp]

    @staticmethod
    def from_path(path: Optional[str]):
        if path:
            path = os.path.expanduser(path)
        p = path or EnvVariables.config_path.resolve(True)

        if p and os.path.exists(p):
            with open(os.path.expanduser(p)) as cp:
                y = ruamel.yaml.load(cp, Loader=ruamel.yaml.Loader)
                return JanisConfiguration(y)

        return JanisConfiguration()

    @staticmethod
    def default():

        return {
            "configdir": EnvVariables.config_dir.resolve(True),
            "environment": {"default": None},
            "outputdir": EnvVariables.exec_dir.resolve(True),
            "searchpaths": [os.path.expanduser("~/janis/")],
        }
