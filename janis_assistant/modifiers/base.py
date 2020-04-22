from abc import ABC
from inspect import isclass
from typing import Dict, List, Tuple, Union, Type

from janis_core import Tool


class PipelineModifierBase(ABC):
    def tool_modifier(self, tool, inputs: Dict, hints: Dict[str, str]) -> Tool:
        return tool

    def inputs_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:
        return inputs

    def apply(
        self, tool: Tool, inputs: Dict, hints: Dict[str, str]
    ) -> Tuple[Tool, Dict]:
        return (
            self.tool_modifier(tool, inputs, hints),
            self.inputs_modifier(tool, inputs, hints),
        )

    @staticmethod
    def apply_many(
        modifiers: List[Union["PipelineModifierBase", Type["PipelineModifierBase"]]],
        wf: Tool,
        inputs: Dict,
        hints: Dict[str, str],
    ) -> Tuple[Tool, Dict]:

        for m in modifiers:
            if isclass(m):
                m = m()
            wf, inputs = m.apply(wf, inputs or {}, hints)

        return wf, inputs
