from os.path import exists
from typing import Dict

from janis_core import (
    Tool,
    File,
    Directory,
    Logger,
    apply_secondary_file_format_to_filename,
    Array,
    TInput,
    DataType,
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
        validate_inputs(wf, inputs)

        # expect fully qualified inputs
        if self.check_file_existence:
            self.check_existence_of_files(wf, inputs)

        return inputs

    @staticmethod
    def check_existence_of_files(wf: Tool, inputs: Dict):

        doesnt_exist = {}

        for inp in wf.tool_inputs():
            intype = inp.intype
            is_path = isinstance(intype, (File, Directory))
            is_array_of_paths = isinstance(intype, Array) and isinstance(
                intype.fundamental_type(), (File, Directory)
            )

            if not (is_path or is_array_of_paths):
                continue

            val = inputs.get(inp.id())
            if val is None:
                if inp.intype.optional:
                    continue
                raise Exception(f"Expected input '{inp.id()}' was not found or is null")

            doesnt_exist.update(InputChecker.check_base_with_type(inp, intype, val))

        if len(doesnt_exist) > 0:
            import ruamel.yaml

            stringified = ruamel.yaml.dump(doesnt_exist, default_flow_style=False)
            raise Exception("The following inputs were not found:\n" + stringified)

    @staticmethod
    def check_base_with_type(inp: TInput, intype: DataType, val, suffix=""):
        doesnt_exist = {}
        if isinstance(intype, Array):
            st = intype.subtype()
            if not isinstance(val, list):
                raise Exception(
                    f"Expected {inp.id()} to be list, but {str(val)} was a {type(val)}"
                )
            for v, idx in zip(val, range(len(val))):
                nsuffix = f"{suffix}_{idx}"
                doesnt_exist.update(
                    InputChecker.check_base_with_type(inp, st, v, suffix=nsuffix)
                )
            return doesnt_exist

        if isinstance(val, list):
            raise Exception(f"Expected singular item for {inp.id()}, received list.")

        if not InputChecker.check_if_input_exists(val):
            doesnt_exist[inp.id() + suffix] = val

        if not isinstance(intype, File):
            return doesnt_exist

        secs = intype.secondary_files() or []
        for sec in secs:
            sec_filename = apply_secondary_file_format_to_filename(val, sec)
            if not InputChecker.check_if_input_exists(sec_filename):
                secsuffix = sec.replace("^", "").replace(".", "")
                doesnt_exist[inp.id() + "_" + secsuffix + suffix] = (
                    "(SECONDARY) " + sec_filename
                )

        return doesnt_exist

    @staticmethod
    def check_if_input_exists(path):
        if not FileScheme.is_local_path(path):
            Logger.warn(f"Skipping as path is not local: '{path}'")
            return True
        return exists(path)
