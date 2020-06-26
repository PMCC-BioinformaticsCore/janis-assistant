from typing import List, Tuple
from datetime import datetime

from janis_assistant.utils.dateutil import DateUtil

from janis_assistant.data.models.base import DatabaseObject
from janis_assistant.data.enums.taskstatus import TaskStatus
from janis_assistant.data.models.inputs import WorkflowInputModel
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.workflowjob import RunJobModel


class RunStatusUpdate(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[Tuple[str, str]]:
        return [
            ("submission_id", "submission_id"),
            ("run_id", "run_id"),
            ("status", "status"),
            ("date", "date"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        status          STRING NOT NULL,
        submission_id   STRING NOT NULL,
        run_id          STRING NOT NULL,
        date            STRING NOT NULL,
        
        PRIMARY KEY(submission_id, run_id, status)
        """

    def __init__(
        self, submission_id: str, run_id: str, status: TaskStatus, date: datetime
    ):
        self.submission_id = submission_id
        self.run_id = run_id
        self.status = status if isinstance(status, TaskStatus) else TaskStatus(status)
        if not isinstance(date, datetime):
            date = DateUtil.parse_iso(date)
        self.date = date


class SubmissionModel(DatabaseObject):
    def __init__(
        self,
        id_: str,
        outdir: str,
        author: str,
        tags: List[str],
        labels: List[str],
        runs: List = None,
    ):
        self.id_ = id_
        self.outdir = outdir
        self.author = author
        self.tags = tags
        self.labels = labels
        self.runs = runs or []

    def keymap(cls) -> List[Tuple[str, str]]:
        return [
            ("id_", "id"),
            ("outdir", "outdir"),
            ("author", "author"),
            ("labels", "labels"),
            ("tags", "tags"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        id          STRING NOT NULL,
        outdir      STRING NOT NULL,
        author      STRING NOT NULL,
        tags        STRING,
        labels      STRING,
        """


class RunModel(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[Tuple[str, str]]:
        return [
            ("id_", "id"),
            ("submission_id", "submission_id"),
            ("engine_id", "engine_id"),
            ("status", "status"),
            ("error", "error"),
            ("labels", "labels"),
            ("tags", "tags"),
            ("last_updated", "last_updated"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        id              STRING NOT NULL,
        submission_id   STRING NOT NULL,
        engine_id       STRING,
        execution_dir   STRING,
        status          STRING,
        error           STRING,
        labels          STRING,
        tags            STRING,
        jobs            STRING,
        inputs          STRING,
        outputs         STRING,
        events          STRING,
        last_updated    STRING,
        
        PRIMARY KEY (id, submission_id)
        """

    # Runs are scoped to a "Submission"
    def __init__(
        self,
        id_: str,
        submission_id: str,
        engine_id: str,
        status: TaskStatus,
        execution_dir: str,
        error: str = None,
        labels: List[str] = None,
        tags: List[str] = None,
        jobs: List[RunJobModel] = None,
        inputs: List[WorkflowInputModel] = None,
        outputs: List[WorkflowOutputModel] = None,
        events: List[RunStatusUpdate] = None,
        last_updated: datetime = None,
    ):
        self.id_ = id_
        self.submission_id = submission_id
        self.engine_id = engine_id
        self.status = status
        self.error = error
        self.labels = labels
        self.tags = tags
        self.jobs = jobs
        self.inputs = inputs
        self.outputs = outputs
        self.events = events
        self.last_updated = last_updated
