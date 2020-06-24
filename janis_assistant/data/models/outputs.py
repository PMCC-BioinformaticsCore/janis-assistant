import json
import datetime
from os.path import commonprefix

from typing import Optional, List

from janis_core.utils.logger import Logger

from janis_assistant.utils import stringify_value_or_array
from janis_assistant.utils.dateutil import DateUtil


class WorkflowOutputModel:
    def __init__(
        self,
        tag: str,
        iscopyable: bool,
        original_path: Optional[str],
        new_path: Optional[str],
        timestamp: Optional[str],
        output_name: Optional[str],
        output_folder: Optional[str],
        secondaries,
        extension,
        value: str = None,
    ):
        self.tag = tag
        self.iscopyable = iscopyable
        self.originalpath = original_path
        self.newpath = new_path

        self.output_name = output_name
        self.output_folder = output_folder

        self.secondaries = secondaries

        if not isinstance(timestamp, datetime.datetime):
            timestamp = DateUtil.parse_iso(timestamp)
        self.timestamp = timestamp
        self.extension = extension
        self.value = value

    @staticmethod
    def safe_parse(value):
        try:
            v = json.loads(value)
            if v is None:
                return None
            return stringify_value_or_array(v)
        except Exception as e:
            Logger.debug(f"Couldn't parse value '{value}' because: {repr(e)}")
            return str(value)

    @staticmethod
    def from_row(row):
        output_name = WorkflowOutputModel.safe_parse(row[6])
        output_folder = WorkflowOutputModel.safe_parse(row[7])
        secondaries = WorkflowOutputModel.safe_parse(row[8])

        return WorkflowOutputModel(
            tag=row[1],
            iscopyable=row[2],
            original_path=row[3],
            new_path=row[4],
            timestamp=row[5],
            output_name=output_name,
            output_folder=output_folder,
            secondaries=secondaries,
            extension=row[9],
        )

    def format(self):
        return f"- {self.tag}: {self._format_value(self.newpath)}"

    @staticmethod
    def _format_value(value, isroot=True):
        if not value:
            return ""

        if isinstance(value, str):
            value = value.split("|")

        if isinstance(value, list):
            if len(value) == 0:
                return ""
            elif len(value) == 1:
                value = value[0]
            else:
                values = [
                    WorkflowOutputModel._format_value(v, isroot=False) for v in value
                ]
                if isroot:
                    return "(multiple) " + commonprefix(values) + "*"
                return commonprefix(values)

        if any(isinstance(value, T) for T in [str, float, int, bool]):
            return str(value)

        # hmmm, idk
        return str(value)
