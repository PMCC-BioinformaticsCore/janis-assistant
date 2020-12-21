from contextlib import contextmanager
from typing import Dict, Union, List

from os import getcwd, chdir
from path import Path

from janis_core import Tool, File, Array, Directory, Logger
from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils import fully_qualify_filename


class InputFileQualifierModifier(PipelineModifierBase):
    """
    InputFileQualifier is designed to take relative file paths from the CWD and turn
    them into fully qualified paths. It also expands path components like '~' or '.'.

    The InputFileQualifier allows you to override the 'cwd' during the init, otherwise
    the current cwd is captured on initialise.
    """

    def __init__(self, cwd=None):
        """
        The current working directory from which to capture relative paths
        :param cwd:
        """
        super().__init__()
        self.cwd = cwd or getcwd()

    def inputs_modifier(self, wf: Tool, inputs: Dict, hints: Dict[str, str]):
        nin = {**inputs}
        inmap = wf.inputs_map()

        # Change the 'cwd' just for the scope of this block
        with Path(self.cwd):
            for tag, value in nin.items():
                if tag not in inmap:
                    # Only localise tags within the inputsdict that are in the tool inputs
                    # This might be a problem later for passthrough inputs (Janis doesn't support yet)
                    continue
                intype = inmap[tag].intype

                # If the type of the input is:
                #   (a) A File / Directory
                isfiletype = isinstance(intype.received_type(), (File, Directory))

                #   (b) Array with a fundamental type (recursively searches if nested array) of File or Directory
                isbasefiletype = isinstance(
                    intype.received_type(), Array
                ) and isinstance(intype.fundamental_type(), (File, Directory))

                # (Skip if not one of these conditions)
                if not (isfiletype or isbasefiletype):
                    try:
                        nin[tag] = intype.coerce_value_if_possible(value)
                    except Exception as e:
                        raise Exception(
                            f"Couldn't coerce the input for '{tag}' ('{value}') to type '{intype}': {e}"
                        )
                    continue

                # Qualify the fully qualify the filepath
                try:
                    nin[tag] = self.fully_qualify_filename_array_or_single(value)
                except Exception as e:
                    raise Exception(
                        f"Couldn't qualify the filename for the input '{tag}' ({value}) to type '{intype}': {e}"
                    )

            return nin

    @staticmethod
    def fully_qualify_filename_array_or_single(
        value: Union[str, List[str], List[List[str]]]
    ):
        """

        :param value:
        :return:
        """
        if isinstance(value, list):
            return [
                InputFileQualifierModifier.fully_qualify_filename_array_or_single(t)
                for t in value
            ]
        return fully_qualify_filename(value)
