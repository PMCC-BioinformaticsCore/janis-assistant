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
        new_inputs = {}

        for inp in tool.tool_inputs():
            modification_required = False

            if isinstance(inp.intype, File) or (
                    isinstance(inp.intype, Array)
                    and isinstance(inp.intype.fundamental_type(), File)
            ):
                if inp.id() in inputs and inputs[inp.id()] is not None:
                    modification_required = True

            if modification_required:
                source = inputs[inp.id()]
                basedir = self.cache_dir
                os.makedirs(basedir, exist_ok=True)

                new_inputs[inp.id()] = self.localise_inputs(
                    inp.id(),
                    inp.intype,
                    basedir,
                    source,
                    # mfranklin 2021-01-08:
                    # if we specify a remote input, and we're localising files, we should localise secondary files
                    localise_secondary_files=True,
                )

        return {**inputs, **new_inputs}
