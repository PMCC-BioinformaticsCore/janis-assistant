from typing import List, Dict, Optional

from janis_core import Tool, DataType
from janis_core.tool.tool import TOutput

from janis_assistant.data.models.util import Serializable


class ValidationRequirements(Serializable):
    def __init__(
        self,
        truthVCF: str,
        reference: str,
        fields: List[str],
        intervals: Optional[str] = None,
    ):
        self.truthVCF = truthVCF
        self.reference = reference
        self.fields = fields
        self.intervals = intervals


def ensure_outputs_are_in_workflow_and_are_compatible(
    tool: Tool, outputs: List[str], compatible_type: DataType
):
    tool_outputs: Dict[str, TOutput] = tool.outputs_map()
    failed_outputs = []
    untyped_outputs = []

    for o in outputs:
        if o not in tool_outputs:
            failed_outputs.append(o)
        elif not compatible_type.can_receive_from(tool_outputs[o].outtype):
            untyped_outputs.append(o)

    return failed_outputs, untyped_outputs
