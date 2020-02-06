from typing import Dict

from janis_core import Tool, File, Array

from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils import fully_qualify_filename


class InputFileQualifierModifier(PipelineModifierBase):
    def inputs_modifier(self, wf: Tool, inputs: Dict, hints: Dict[str, str]):
        nin = {**inputs}
        inmap = wf.inputs_map()
        for tag, value in nin.items():
            if tag not in inmap:
                continue
            intype = inmap[tag].intype

            isfiletype = isinstance(intype.received_type(), File)
            isbasefiletype = isinstance(intype.received_type(), Array) and isinstance(
                intype.fundamental_type(), File
            )
            if not (isfiletype or isbasefiletype):
                continue

            nin[tag] = self.fully_qualify_filename_array_or_single(value)

        return nin

    @staticmethod
    def fully_qualify_filename_array_or_single(value):
        if isinstance(value, list):
            return [
                InputFileQualifierModifier.fully_qualify_filename_array_or_single(t)
                for t in value
            ]
        return fully_qualify_filename(value)
