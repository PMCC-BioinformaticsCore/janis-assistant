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
            subtype = intype.subtype()
            if not isinstance(val, list):
                raise Exception(
                    f"Expected {inp.id()} to be list, but {str(val)} was a {type(val)}"
                )
            for innerval, idx in zip(val, range(len(val))):
                nsuffix = f"{suffix}[{idx}]"
                doesnt_exist.update(
                    InputChecker.check_base_with_type(
                        inp, subtype, innerval, suffix=nsuffix
                    )
                )
            return doesnt_exist

        inpid = inp.id() + suffix

        if isinstance(val, list):
            raise Exception(f"Expected singular item for {inp.id()}, received list.")

        fs = FileScheme.get_filescheme_for_url(val)

        if not fs.exists(val):
            doesnt_exist[inpid] = val

        if not isinstance(intype, File):
            return doesnt_exist

        InputChecker.check_extensions(inpid, intype, val)

        secs = intype.secondary_files() or []
        for sec in secs:
            sec_filename = apply_secondary_file_format_to_filename(val, sec)
            if not InputChecker.check_if_input_exists(fs, sec_filename):
                secsuffix = sec.replace("^", "").replace(".", "")
                doesnt_exist[inp.id() + "_" + secsuffix + suffix] = (
                    "(SECONDARY) " + sec_filename
                )

        return doesnt_exist

    @staticmethod
    def check_if_input_exists(fs: FileScheme, path: str):
        return fs.exists(path)

    @staticmethod
    def check_extensions(inpid: str, datatype: DataType, path: str):
        """
        This method only WARNS about incorrect extension
        """

        if not isinstance(datatype, File):
            return

        if not isinstance(path, str):
            Logger.warn(
                f"Expecting string type input '{inpid}' of file File, but received '{type(path)}'"
            )

        # check extension (and in future, secondaries)
        pre_extensions = [
            datatype.extension,
            *list(datatype.alternate_extensions or []),
        ]
        extensions = {ext for ext in pre_extensions if ext is not None}

        if len(extensions) == 0:
            # skip because no extension
            return

        has_extension = False
        for ext in extensions:
            if path.endswith(ext):
                has_extension = True
                break

        if has_extension:
            # looks like we're sweet
            Logger.debug(
                f"Validated that the input for {inpid} had the expected extension for {datatype.id()}"
            )
            return

        Logger.warn(
            f"The input for '{inpid}' ({datatype.name()}) did not have the expected extension "
            f"{' OR '.join(extensions)}: {path}. "
        )
