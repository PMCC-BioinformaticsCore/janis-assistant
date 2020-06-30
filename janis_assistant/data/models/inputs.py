import json
from typing import List, Tuple

from janis_core.utils.logger import Logger

from janis_assistant.data.models.base import DatabaseObject


class WorkflowInputModel(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[Tuple[str, str]]:
        return [
            ("id_", "id"),
            ("submission_id", "submission_id"),
            ("run_id", "run_id"),
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
        size            INTEGER,
        
        PRIMARY KEY(id, submission_id, run_id)
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
