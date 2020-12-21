import os
import hashlib
from abc import ABC
from inspect import isclass
from typing import Dict, List, Tuple, Union, Type, Optional

from janis_assistant.management.filescheme import FileScheme, LocalFileScheme
from janis_core import Tool, Logger, DataType, apply_secondary_file_format_to_filename


class PipelineModifierBase(ABC):
    def tool_modifier(self, tool, inputs: Dict, hints: Dict[str, str]) -> Tool:
        return tool

    def inputs_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:
        return inputs

    def apply(
        self, tool: Tool, inputs: Dict, hints: Dict[str, str]
    ) -> Tuple[Tool, Dict]:
        return (
            self.tool_modifier(tool, inputs, hints),
            self.inputs_modifier(tool, inputs, hints),
        )

    @staticmethod
    def apply_many(
        modifiers: List[Union["PipelineModifierBase", Type["PipelineModifierBase"]]],
        wf: Tool,
        inputs: Dict,
        hints: Dict[str, str],
    ) -> Tuple[Tool, Dict]:

        for m in modifiers:
            if isclass(m):
                m = m()
            wf, inputs = m.apply(wf, inputs or {}, hints)

        return wf, inputs


class FileLocatorModifierBase(PipelineModifierBase):
    def localise_inputs(
        self,
        inpid: str,
        inptype: DataType,
        dest_dir: str,
        source: Union[str, List[str]],
        localise_secondary_files: bool = True,
    ):
        if isinstance(source, list):
            return [self.localise_inputs(inpid, inptype, dest_dir, s) for s in source]

        fs = FileScheme.get_type_by_prefix(source)()
        if isinstance(fs, LocalFileScheme):
            return source

        out_path = self.generate_file_path(source, dest_dir)
        if os.path.exists(out_path):
            Logger.info(
                f"A file already exists when localising '{inpid}' at '{out_path}'. If this isn't the right file, "
                f"you'll need to manually remove this file before proceeding"
            )
        else:
            try:
                Logger.info(f"Downloading file from {source} -> {out_path}")
                fs.cp_from(source, out_path)
            except Exception as e:
                Logger.critical(
                    f"Couldn't localise source from {source} -> {out_path}: {repr(e)}"
                )
                raise

        if localise_secondary_files:
            try:
                for sec in inptype.secondary_files() or []:
                    sec_source = apply_secondary_file_format_to_filename(source, sec)
                    out_sec_path = apply_secondary_file_format_to_filename(
                        out_path, sec
                    )

                    if os.path.exists(out_sec_path):
                        Logger.info(
                            f"The secondary file for {inpid} ({sec}) already exists when localising '{inpid}' at '{out_sec_path}'. If this isn't the right file, "
                            f"you'll need to manually remove this file before proceeding"
                        )
                    elif not fs.exists(sec_source):
                        Logger.warn(
                            f"Couldn't find the secondary file for {inpid}, expected at {sec_source}, skipping for now"
                        )
                    else:
                        fs.cp_from(sec_source, out_sec_path)

            except Exception as e:
                Logger.critical(f"Couldn't localise secondary file due to: {e}")

        return out_path

    @classmethod
    def generate_file_path(cls, source: str, dest_dir: str):
        fs = FileScheme.get_type_by_prefix(source)()
        date_modified = fs.last_modified(source)
        local_filename = (
            f"{cls.hash_filename(source, date_modified)}_{os.path.basename(source)}"
        )

        return os.path.join(dest_dir, local_filename)

    @classmethod
    def hash_filename(cls, source: str, date_modified: Optional[str] = None):
        components = source + (date_modified or "")
        hash_md5 = hashlib.md5(str.encode(components))
        hashed_filename = hash_md5.hexdigest()

        return hashed_filename
