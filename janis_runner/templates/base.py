from abc import ABC, abstractmethod

from janis_core import Logger

from janis_runner.engines.enginetypes import EngineType


class EnvironmentTemplate(ABC):

    default_recipes = {}

    def __init__(
        self, additionalRecipes=None, default_recipe=None, recipe_locations=None
    ):

        self.recipes = {**self.default_recipes, **(additionalRecipes or {})}
        self.default_recipe = default_recipe
        self.recipe_locations = []
        if recipe_locations:
            self.recipe_locations = (
                recipe_locations
                if isinstance(recipe_locations, list)
                else [recipe_locations]
            )

        self._loaded_recipes = False

    def get_recipe_for_key(self, key):

        if not self._loaded_recipes and self.recipe_locations:
            import ruamel.yaml

            for recipe_location in self.recipe_locations:
                try:
                    with open(recipe_location) as rl:
                        adr = ruamel.yaml.load(rl, Loader=ruamel.yaml.Loader)
                        self.recipes.update(adr)

                except Exception as e:
                    Logger.critical(f"Couldn't load recipe '{recipe_location}': {e}")

            self._loaded_recipes = True

        if key is None:
            return {}
        if key in self.recipes:
            return self.recipes[key] or {}

        raise KeyError(
            f"Couldn't find recipe '{key}' in recipes, expected one of: {', '.join(self.recipes.keys())}"
        )

    @abstractmethod
    def engine_config(self, engine: EngineType):
        pass

    def preengine_code(self, engine: EngineType):
        pass
