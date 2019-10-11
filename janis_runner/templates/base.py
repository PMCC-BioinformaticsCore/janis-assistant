from abc import ABC, abstractmethod

from janis_runner.engines.enginetypes import EngineType


class EnvironmentTemplate(ABC):

    default_recipes = {}

    def __init__(self, additionalRecipes=None, default_recipe=None):

        self.recipes = {**self.default_recipes, **(additionalRecipes or {})}
        self.default_recipe = default_recipe

    def get_recipe_for_key(self, key):
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
