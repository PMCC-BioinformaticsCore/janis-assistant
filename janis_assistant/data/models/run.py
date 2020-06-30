from typing import List, Tuple, Union, Optional
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


class RunModel(DatabaseObject):
    DEFAULT_ID = "<default>"

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
        last_updated    STRING,

        PRIMARY KEY (id, submission_id),
        FOREIGN KEY (submission_id) REFERENCES sumissions(id)

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
        last_updated: datetime = None,
        # extra fields not populated by DB
        jobs: List[RunJobModel] = None,
        inputs: List[WorkflowInputModel] = None,
        outputs: List[WorkflowOutputModel] = None,
        events: List[RunStatusUpdate] = None,
    ):
        self.id_ = id_
        self.submission_id = submission_id
        self.execution_dir = execution_dir
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


class SubmissionModel(DatabaseObject):
    def __init__(
        self,
        id_: str,
        outdir: str,
        author: str,
        labels: List[str],
        tags: List[str],
        timestamp: Union[str, datetime],
        engine_type: str,
        # metadata not populated directly by DB, but might be used for formatting
        engine_url: Optional[str] = None,
        runs: List[RunModel] = None,
        # From previous WorkflowModel, some data is now derived:
        #   runs:
        #       - Start / finish times
        #       - Errors
        #       - Last updated
    ):
        self.id_ = id_
        self.outdir = outdir
        self.author = author
        self.tags = tags
        self.labels = labels
        if not isinstance(timestamp, datetime):
            timestamp = DateUtil.parse_iso(timestamp)
        self.timestamp = timestamp
        self.engine_type = engine_type
        # other things
        self.runs = runs or []
        self.engine_url = engine_url

    def keymap(cls) -> List[Tuple[str, str]]:
        return [
            ("id_", "id"),
            ("outdir", "outdir"),
            ("author", "author"),
            ("labels", "labels"),
            ("tags", "tags"),
            ("timestamp", "timestamp"),
            ("engine_type", "engine"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        id          STRING PRIMARY KEY,
        outdir      STRING NOT NULL,
        author      STRING NOT NULL,
        tags        STRING,
        labels      STRING,
        timestamp   STRING,
        engine      STRING,
        """
