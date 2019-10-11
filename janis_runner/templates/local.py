import ruamel.yaml
from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_runner.templates.base import EnvironmentTemplate
from janis_runner.engines.enginetypes import EngineType


class LocalTemplate(EnvironmentTemplate):

    default_recipes = {}

    def __init__(
        self, additionalRecipes=None, recipeLocations=None, defaultRecipe=None
    ):

        super().__init__(
            additionalRecipes=additionalRecipes,
            default_recipe=defaultRecipe,
            recipe_locations=recipeLocations,
        )

    def cromwell(self):
        return None

    def engine_config(self, engine: EngineType):
        return None
