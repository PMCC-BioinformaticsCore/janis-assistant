from typing import Dict
from janis_core import Tool, TInput, DataType, File, Array, Directory, Logger

from janis_assistant.modifiers.base import PipelineModifierBase


class CwlInputObjectUnwrapperModifier(PipelineModifierBase):
    def inputs_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:
        new_inputs = {}
        for inp in tool.inputs_map().values():
            if inp.id() not in inputs:
                continue
            new_inputs[inp.id()] = self.process_single_input(
                inp.id(), inp.intype, inputs[inp.id()]
            )

        return {**inputs, **new_inputs}

    def process_single_input(self, key: str, dt: DataType, value):
        if value is None:
            return None

        if isinstance(value, list):
            if not isinstance(dt, Array):
                Logger.warn(f"{key} provided list of values, but type was not an array")
                subtype = dt
            else:
                subtype = dt.subtype()
            return [
                self.process_single_input(f"{key}.{idx}", subtype, value[idx])
                for idx in range(len(value))
            ]

        if not isinstance(value, dict):
            return value

        if dt.is_base_type((File, Directory)):
            if "path" in value:
                return value["path"]
            else:
                Logger.warn(
                    f"Couldn't unwrap dictionary for input {key} ('{value}') as it didn't provide a value for 'path'"
                )
        else:
            Logger.warn(
                f"Couldn't unwrap dictionary for input {key} ('{value}') as the input isn't expected to be a file"
            )
        return value
