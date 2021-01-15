import os.path
from typing import Dict, List, Union, Optional

from janis_assistant.management.filescheme import FileScheme, LocalFileScheme
from janis_core import (
    Tool,
    WorkflowBase,
    Logger,
    File,
    Array,
    DataType,
    apply_secondary_file_format_to_filename,
    InputDocumentation,
)

from janis_assistant.modifiers.base import FileLocatorModifierBase


class RemoteFileLocatorModifier(FileLocatorModifierBase):
    """
    Find and download remote files
    """

    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir

    def inputs_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:
        """
        Download remote files and reploce input with the local files

        :param tool: an instance of janis tool
        :type tool: janis_core.Tool
        :param inputs: a dictionary of tool inputs
        :type inputs: dict
        :param hints:
        :type hints: dict
        :return: modified input
        :rtype: dict
        """
        if not isinstance(tool, WorkflowBase):
            return inputs

        wf: WorkflowBase = tool
        new_inputs = {}

        for inpnode in wf.input_nodes.values():
            modification_required = False

            if isinstance(inpnode.datatype, File) or (
                isinstance(inpnode.datatype, Array)
                and isinstance(inpnode.datatype.fundamental_type(), File)
            ):
                if inpnode.id() in inputs and inputs[inpnode.id()] is not None:
                    modification_required = True

            if modification_required:
                doc: InputDocumentation = inpnode.doc
                source = inputs[inpnode.id()]
                basedir = self.cache_dir
                os.makedirs(basedir, exist_ok=True)

                new_inputs[inpnode.id()] = self.localise_inputs(
                    inpnode.id(),
                    inpnode.datatype,
                    basedir,
                    source,
                    # mfranklin 2021-01-08:
                    # if we specify a remote input, and we're localising files, we should localise secondary files
                    localise_secondary_files=True,
                )

        return {**inputs, **new_inputs}
