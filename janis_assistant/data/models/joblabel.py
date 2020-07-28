from typing import List, Union

from .base import DatabaseObject, DatabaseObjectField


class JobLabelModel(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[DatabaseObjectField]:
        return [
            DatabaseObjectField("submission_id", is_primary=True),
            DatabaseObjectField("run_id", is_primary=True),
            DatabaseObjectField("job_id", is_primary=True),
            DatabaseObjectField("labels", encode=True),
        ]

    @classmethod
    def table_schema(cls):
        return """
        submission_id   STRING NOT NULL,
        run_id          STRING NOT NULL,
        job_id          STRING NOT NULL,
        labels          STRING
        """

    def __init__(
        self,
        submission_id: str,
        run_id: str,
        job_id: str,
        labels: Union[str, List[str]],
    ):
        self.submission_id = submission_id
        self.run_id = run_id
        self.job_id = job_id
        self.labels = labels if isinstance(labels, list) else [labels]
