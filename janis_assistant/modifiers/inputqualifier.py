from typing import Dict

from janis_core import Tool, File

from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils import fully_qualify_filename


class InputFileQualifierModifier(PipelineModifierBase):
    def inputs_modifier(self, wf: Tool, inputs: Dict, hints: Dict[str, str]):
        nin = {**inputs}
        inmap = wf.inputs_map()
        for tag, value in nin.items():
            if tag not in inmap:
                continue
            if not isinstance(inmap[tag].intype.received_type(), File):
                continue

            nin[tag] = fully_qualify_filename(value)

        return nin
