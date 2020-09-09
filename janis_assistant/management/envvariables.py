from enum import Enum
from os import getenv


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
    base_dir = "JANIS_BASEDIR"
    exec_dir = "JANIS_EXCECUTIONDIR"
    output_dir = "JANIS_OUTPUTDIR"
    search_path = "JANIS_SEARCHPATH"
    recipe_paths = "JANIS_RECIPEPATHS"
    recipe_directory = "JANIS_RECIPEDIRECTORY"  # secretly comma separated
    db_script_generator = "JANIS_DBCREDENTIALSGENERATOR"
    db_script_generator_cleanup = "JANIS_DBCREDENTIALSGENERATORCLEANUP"

    cromwelljar = "JANIS_CROMWELLJAR"

    def __str__(self):
        return self.value

    def default(self):
        import os

        if self == EnvVariables.config_dir:
            return os.path.join(os.getenv("HOME"), ".janis/")
        if self == EnvVariables.base_dir:
            return os.path.join(os.getenv("HOME"), "janis")
        if self == EnvVariables.output_dir:
            return None
        elif self == EnvVariables.exec_dir:
            return os.path.join(
                EnvVariables.base_dir.resolve(include_default=True), "execution/"
            )
        elif self == EnvVariables.config_path:
            return os.path.join(EnvVariables.config_dir.resolve(True), "janis.conf")
        elif self == EnvVariables.recipe_paths:
            return []
        elif self == EnvVariables.recipe_directory:
            return []

        raise Exception(f"Couldn't determine default() for '{self.value}'")

    def resolve(self, include_default=False):
        value = getenv(self.value)
        if value is None and include_default:
            value = self.default()
        if self == EnvVariables.recipe_paths:
            return value.split(",") if value else None
        if self == EnvVariables.recipe_directory:
            return value.split(",") if value else None
        return value
