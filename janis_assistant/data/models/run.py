from typing import List, Tuple, Union, Optional
from datetime import datetime

from janis_assistant.utils import second_formatter

from janis_assistant.utils.dateutils import DateUtil

from janis_assistant.data.models.base import DatabaseObject, DatabaseObjectField
from janis_assistant.data.enums.taskstatus import TaskStatus
from janis_assistant.data.models.inputs import WorkflowInputModel
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.workflowjob import RunJobModel


class RunStatusUpdate(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[DatabaseObjectField]:
        return [
            DatabaseObjectField("submission_id", is_primary=True),
            DatabaseObjectField("run_id", is_primary=True),
            DatabaseObjectField("status", is_primary=True),
            DatabaseObjectField("date"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        status          STRING NOT NULL,
        submission_id   STRING NOT NULL,
        run_id          STRING NOT NULL,
        date            STRING NOT NULL
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
    def keymap(cls) -> List[DatabaseObjectField]:
        return [
            DatabaseObjectField("id_", "id", is_primary=True),
            DatabaseObjectField("submission_id", is_primary=True),
            DatabaseObjectField("engine_id"),
            DatabaseObjectField("execution_dir"),
            DatabaseObjectField("status"),
            DatabaseObjectField("error"),
            DatabaseObjectField("labels", encode=True),
            DatabaseObjectField("tags", encode=True),
            DatabaseObjectField("last_updated"),
            DatabaseObjectField("name"),
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
        name            STRING,
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
        name: str,
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
        self.name = name
        self.status = status
        self.error = error
        self.labels = labels
        self.tags = tags
        self.jobs = jobs
        self.inputs = inputs
        self.outputs = outputs

        if isinstance(last_updated, str):
            last_updated = DateUtil.parse_iso(last_updated)

        self.last_updated = last_updated

        # to be calculated
        self.start = None
        self.finish = None

        # call setters
        self.events = None
        self.set_events(events)

    def set_events(self, events):
        self.events = events or []

        if len(self.events) == 0:
            return

        self.status = sorted(self.events, key=lambda e: e.date)[-1].status

        self.start = min(e.date for e in self.events)
        final_status = [e.date for e in self.events if e.status.is_in_final_state()]
        if len(final_status) > 0:
            self.finish = final_status[0]

    def apply_ids_to_children(self):
        arrays = [self.jobs, self.inputs, self.outputs, self.events]

        for ar in arrays:
            if not ar:
                continue
            for el in ar:
                el.set_ids(submission_id=self.submission_id, run_id=self.id_)

    def format(self, tb, **kwargs):
        if not self.jobs:
            return ""
        nl = "\n"

        return nl.join(
            j.format(tb, njobs_in_parent=len(self.jobs), **kwargs)
            for j in sorted(self.jobs, key=lambda j: j.start or DateUtil.now())
        )


class SubmissionModel(DatabaseObject):
    def __init__(
        self,
        id_: str,
        output_dir: str,
        execution_dir: str,
        author: str,
        labels: List[str],
        tags: List[str],
        timestamp: Union[str, datetime],
        engine_type: str,
        # metadata not populated directly by DB, but might be used for formatting
        engine_url: Optional[str] = None,
        runs: List[RunModel] = None,
        name: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        error: Optional[str] = None
        # From previous WorkflowModel, some data is now derived:
        #   runs:
        #       - Start / finish times
        #       - Errors
        #       - Last updated
    ):
        self.id_ = id_
        self.output_dir = output_dir
        self.execution_dir = execution_dir
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
        self.name = name
        self.status = status
        self.error = error

    def get_names(self):
        if self.name:
            return self.name
        if not self.runs:
            return "N/A"
        return ", ".join(set(r.name for r in self.runs if r.name))

    @classmethod
    def keymap(cls) -> List[DatabaseObjectField]:
        return [
            DatabaseObjectField("id_", "id", is_primary=True),
            DatabaseObjectField("output_dir"),
            DatabaseObjectField("execution_dir", dbalias="exec_dir"),
            DatabaseObjectField("author"),
            DatabaseObjectField("labels", encode=True),
            DatabaseObjectField("tags", encode=True),
            DatabaseObjectField("timestamp"),
            DatabaseObjectField("engine_type", "engine"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        id          STRING NOT NULL,
        output_dir  STRING NOT NULL,
        exec_dir    STRING NOT NULL,
        author      STRING NOT NULL,
        tags        STRING,
        labels      STRING,
        timestamp   STRING,
        engine      STRING,
        """

    def format(self, **kwargs):
        tb = "    "
        nl = "\n"

        start, finish = None, DateUtil.now()
        last_updated = None
        if self.runs:
            start_times = [s.start for s in self.runs if s.start]
            finish_times = [s.finish for s in self.runs if s.finish]
            last_updated_times = [s.last_updated for s in self.runs if s.last_updated]

            if start_times:
                start = min(start_times)
            if finish_times and len(finish_times) == len(self.runs):
                finish = max(finish_times)
            if last_updated_times:
                last_updated = max(last_updated_times)

        fin = finish or DateUtil.now()

        duration = round((fin - start).total_seconds()) if start else 0

        updated_text = "Unknown"
        if last_updated:
            secs_ago = int((DateUtil.now() - last_updated).total_seconds())
            if secs_ago > 2:
                updated_text = second_formatter(secs_ago) + " ago"
            else:
                updated_text = "Just now"
            updated_text += f" ({last_updated.replace(microsecond=0).isoformat()})"

        engine_ids, rstatuses, errors = "", "", []

        if self.error:
            errors.append(f"GLOBAL: {self.error}")

        if self.runs:
            engine_ids = ", ".join(r.engine_id for r in self.runs if r.engine_id)
            errors.extend(r.error for r in self.runs if r.error)

            rstatuses = ", ".join(
                str(r.status.to_string()) for r in self.runs if r.status
            )

        statuses = self.status.to_string() if self.status else rstatuses
        ers = "\n".join(errors)

        return f"""\
SID:        {self.id_}
EngId:      {engine_ids}
Engine:     {self.engine_type}

Task Dir:   {self.output_dir}
Exec Dir:   {self.execution_dir}

Name:       {self.get_names() or 'N/A'} 
Status:     {statuses}
Duration:   {second_formatter(duration)}
Start:      {start.isoformat() if start else 'N/A'}
Finish:     {finish.isoformat() if finish else "N/A"}
Updated:    {updated_text}

Jobs: 
{nl.join(j.format(tb, **kwargs) for j in sorted(self.runs, key=lambda j: j.start or DateUtil.now()))}       


{("Error: " + ers) if ers else ''}
    """.strip()


# {("Outputs:" + "".join(nl + tb + o.format() for o in self.outputs) if self.outputs else '')}
