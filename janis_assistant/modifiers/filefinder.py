from typing import Dict

from janis_core import Tool, WorkflowBase

from janis_assistant.modifiers.base import PipelineModifierBase


class FileFinderModifier(PipelineModifierBase):
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir

    # def inputs_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:
    #
    #     if not isinstance(tool, WorkflowBase):
    #         return inputs
    #
    #     wf: WorkflowBase = tool
    #
    #     for inpnode in wf.input_nodes.values():
    #         if inpnode.id() in inputs and inputs[inpnode.id()] is not None:
    #             continue
    #         elif inpnode.default is not None or inpnode.datatype.optional:
    #             continue
    #
    #         if inpnode.doc.example:
    #
    #
    #     elif isinstance(inpnode.datatype, Fasta):
    #     find_fasta_files(inpnode)
    #
    #     return inputs
