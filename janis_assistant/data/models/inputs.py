import json
from typing import List, Tuple, Union

from janis_core.utils.logger import Logger

from janis_assistant.data.models.base import DatabaseObject


class WorkflowInputModel(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[Union[Tuple[str, str, bool], Tuple[str, str]]]:
        return [
            ("id_", "id", True),
            ("submission_id", "submission_id", True),
            ("run_id", "run_id", True),
            ("value", "value"),
            ("size", "size"),  # bytes
        ]

    @classmethod
    def table_schema(cls):
        return """
        id              STRING NOT NULL,      
        submission_id   STRING NOT NULL,
        run_id          STRING NOT NULL,
        value           STRING,
        size            INTEGER
        """

    def __init__(
        self, id_: str, submission_id: str, run_id: str, value, size: int = None,
    ):
        """

        :param id_: The input TAG
        :param submission_id: The submissionID of the run
        :param run_id: The runID of the input
        :param value: The jsonified value
        :param size: The size of the file in BYTES (nullable)
        """
        self.id_ = id_
        self.submission_id = submission_id
        self.run_id = run_id
        self.value = value
        self.size = size
