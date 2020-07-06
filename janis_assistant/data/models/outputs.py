import json
import datetime
from os.path import commonprefix

from typing import Optional, List, Tuple, Union

from janis_core.utils.logger import Logger

from janis_assistant.data.models.base import DatabaseObject
from janis_assistant.utils import stringify_value_or_array
from janis_assistant.utils.dateutil import DateUtil


class WorkflowOutputModel(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[Union[Tuple[str, str, bool], Tuple[str, str]]]:
        return [
            ("id_", "id", True),
            ("submission_id", "submission_id", True),
            ("run_id", "run_id", True),
            ("output_name", "output_name"),
            ("output_folder", "output_folder"),
            ("secondaries", "secondaries"),
            ("extension", "extension"),
            ("is_copyable", "is_copyable"),
            ("original_path", "original_path"),
            ("new_path", "new_path"),
            ("timestamp", "timestamp"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        id              STRING NOT NULL,      
        submission_id   STRING NOT NULL,
        run_id          STRING NOT NULL,
        
        output_name     STRING,
        output_folder   STRING,
        secondaries     STRING,
        extension       STRING,
        
        is_copyable     BIT,
        original_path   STRING,
        new_path        STRING,
        timestamp       NULLABLE STRING
        """

    def __init__(
        self,
        id_: str,
        submission_id: str,
        run_id: str,
        output_name: Optional[str],
        output_folder: Optional[str],
        secondaries: Optional[List[str]],
        extension: Optional[str],
        is_copyable: bool,
        original_path: Optional[str],
        new_path: Optional[str],
        timestamp: Optional[Union[str, datetime.datetime]],
    ):
        self.id_ = id_
        self.submission_id = submission_id
        self.run_id = run_id
        self.output_name = stringify_value_or_array(output_name)
        self.output_folder = stringify_value_or_array(output_folder)
        self.secondaries = secondaries
        self.extension = extension
        self.is_copyable = is_copyable
        self.original_path = original_path
        self.new_path = new_path

        if not isinstance(timestamp, datetime.datetime):
            timestamp = DateUtil.parse_iso(timestamp)
        self.timestamp = timestamp

    def format(self):
        return f"- {self.id_}: {self._format_value(self.new_path)}"

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
