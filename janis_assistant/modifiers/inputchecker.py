from os.path import exists
from typing import Dict

from janis_core import (
    Tool,
    File,
    Directory,
    Logger,
    apply_secondary_file_format_to_filename,
)

from janis_assistant.management.filescheme import FileScheme
from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils import validate_inputs


class InputChecker(PipelineModifierBase):
    """
    InputChecker is designed to check the existence and validity of inputs.
    The file-existence currently only works for LOCAL files. (prefix: '/' or '.' or 'file://),
    and can be disabled with check_file_existence=False (or --skip-file-check)
    """

    def __init__(self, check_file_existence=True):
        self.check_file_existence = check_file_existence

    def inputs_modifier(self, wf: Tool, inputs: Dict, hints: Dict[str, str]):
        # expect fully qualified inputs
        if self.check_file_existence:
            self.check_existence_of_files(wf, inputs)

        validate_inputs(wf, inputs)

        return inputs

    @staticmethod
    def check_existence_of_files(wf: Tool, inputs: Dict):

        doesnt_exist = {}

        for inp in wf.tool_inputs():
            if not isinstance(inp.intype, (File, Directory)):
                continue

            val = inputs.get(inp.id())
            if val is None:
                if inp.intype.optional:
                    continue
                raise Exception(f"Expected input '{inp.id()}' was not found or is null")

            if not InputChecker.check_if_input_exists(val):
                doesnt_exist[inp.id()] = val

            if isinstance(inp.intype, Directory):
                continue

            secs = inp.intype.secondary_files() or []
            for sec in secs:
                sec_filename = apply_secondary_file_format_to_filename(val, sec)
                if not InputChecker.check_if_input_exists(sec_filename):
                    suffix = sec.replace("^", "").replace(".", "")
                    doesnt_exist[inp.id() + "_" + suffix] = (
                        "(SECONDARY) " + sec_filename
                    )

        if len(doesnt_exist) > 0:
            import ruamel.yaml

            stringified = ruamel.yaml.dump(doesnt_exist, default_flow_style=False)
            raise Exception("The following inputs were not found:\n" + stringified)

    @staticmethod
    def check_if_input_exists(path):
        if not FileScheme.is_local_path(path):
            Logger.warn(f"Skipping as path is not local: '{path}'")
            return True
        return exists(path)
