from enum import Enum
from os import getenv, path, getcwd


class HashableEnum(str, Enum):
    def __str__(self):
        return self.value

    def to_yaml(self):
        return self.value

    pass
    # def __hash__(self):
    #     return self.value.__hash__()


def try_get_home_dir():
    try:
        return getenv("HOME")
    except:
        try:
            return path.expanduser("~")
        except:
            return getcwd()


class EnvVariables(HashableEnum):
    #: Default template to use, NB this template should have NO required arguments.
    default_template = "JANIS_DEFAULTTEMPLATE"

    #: (Default: ~/.janis) Directory of default Janis settings
    config_dir = "JANIS_CONFIGDIR"
    #: (Default: ``$JANIS_CONFIGDIR/janis.conf``) Default configuration file for Janis
    config_path = "JANIS_CONFIGPATH"
    #: Use this directory as a BASE to generate a new output directory for each Janis run
    output_dir = "JANIS_OUTPUTDIR"
    #: Use this directory for intermediate files
    exec_dir = "JANIS_EXCECUTIONDIR"
    #: Additional search paths (comma separated) to lookup Janis workflows in
    search_path = "JANIS_SEARCHPATH"
    #: List of YAML recipe files (comma separated) for Janis to consume, See the RECIPES section for more information.
    recipe_paths = "JANIS_RECIPEPATHS"
    #: Directories for which each file (ending in .yaml | .yml) is a key of input values. See the RECIPES section for more information.
    recipe_directory = "JANIS_RECIPEDIRECTORY"  # secretly comma separated

    #: Override the Cromwell JAR that Janis uses
    cromwelljar = "JANIS_CROMWELLJAR"

    db_script_generator = "JANIS_DBCREDENTIALSGENERATOR"
    db_script_generator_cleanup = "JANIS_DBCREDENTIALSGENERATORCLEANUP"

    def __str__(self):
        return self.value

    def default(self):
        import os.path

        HOME = try_get_home_dir()

        if self == EnvVariables.config_dir:
            return os.path.join(HOME, ".janis/")
        if self == EnvVariables.output_dir:
            return None
        elif self == EnvVariables.exec_dir:
            return os.path.join(HOME, "janis", "execution/")
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
