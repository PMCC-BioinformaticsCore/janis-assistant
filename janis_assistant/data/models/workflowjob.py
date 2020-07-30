from datetime import datetime
from typing import Optional, Union, List, Tuple

from janis_assistant.data.enums.taskstatus import TaskStatus
from janis_assistant.data.models.base import DatabaseObject, DatabaseObjectField
from janis_assistant.utils import second_formatter
from janis_assistant.utils.dateutil import DateUtil
from janis_core.utils.logger import _bcolors


class RunJobModel(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[DatabaseObjectField]:
        return [
            DatabaseObjectField("id_", "id", is_id_key=True),
            DatabaseObjectField("submission_id", is_id_key=True),
            DatabaseObjectField("run_id", is_id_key=True),
            DatabaseObjectField("parent"),
            DatabaseObjectField("name"),
            DatabaseObjectField("batchid"),
            DatabaseObjectField("shard"),
            DatabaseObjectField("attempt"),
            DatabaseObjectField("container"),
            DatabaseObjectField("status"),
            DatabaseObjectField("start"),
            DatabaseObjectField("finish"),
            DatabaseObjectField("backend"),
            DatabaseObjectField("cached"),
            DatabaseObjectField("stdout"),
            DatabaseObjectField("stderr"),
            DatabaseObjectField("script"),
            DatabaseObjectField("memory"),
            DatabaseObjectField("cpu"),
            DatabaseObjectField("analysis"),
        ]

    @classmethod
    def table_schema(cls):
        return """
id              STRING NOT NULL,
submission_id   STRING NOT NULL,
run_id          STRING NOT NULL,
parent          NULLABLE STRING,
shard           NULLABLE INT,
attempt         NULLABLE INT,

name            STRING,
batchid         STRING,
container       STRING,
status          STRING,
start           STRING,
finish          NULLABLE STRING,
backend         STRING,
cached          BOOLEAN,
stdout          STRING,
stderr          STRING,
script          STRING,

memory          STRING,
cpu             STRING,
analysis        STRING,
    """

    def __init__(
        self,
        id_: str,
        submission_id: str,
        run_id: str,
        parent: Optional[str],
        name: str,
        batchid: Optional[str] = None,
        shard: Optional[int] = None,
        attempt: Optional[int] = None,
        container: Optional[str] = None,
        status: TaskStatus = None,
        start: Union[str, datetime] = None,
        finish: Optional[Union[str, datetime]] = None,
        backend: Optional[str] = None,
        cached: bool = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        script: Optional[str] = None,
        memory: Optional[str] = None,
        cpu: Optional[str] = None,
        analysis: Optional[str] = None,
        # Optional
        jobs: Optional[list] = None,
    ):
        self.id_ = id_
        self.submission_id = submission_id
        self.run_id = run_id
        self.parent = parent

        self.status = status if isinstance(status, TaskStatus) else TaskStatus(status)

        self.name = name
        self.batchid = batchid
        self.shard = None
        if shard is not None:
            if isinstance(shard, str) and shard.isdigit:
                shard = int(shard)
            if shard >= 0:
                self.shard = shard

        self.attempt = None
        if attempt is not None:
            if isinstance(attempt, str) and attempt.isdigit():
                attempt = int(attempt)
            if attempt > 1:
                self.attempt = attempt

        self.container = container

        self.backend = backend
        self.cached = cached

        self.stderr = stderr
        self.stdout = stdout

        self.start = start
        self.finish = finish
        if start and isinstance(start, str):
            self.start = DateUtil.parse_iso(start)
        if finish and isinstance(finish, str):
            self.finish = DateUtil.parse_iso(finish)

        self.script = script
        self.memory = memory
        self.cpu = cpu
        self.analysis = analysis

        self.jobs: Optional[List[RunJobModel]] = jobs or None
        self.events = None

    def set_ids(self, submission_id, run_id):
        self.submission_id = submission_id
        self.run_id = run_id

        if self.jobs:
            for j in self.jobs:
                j.set_ids(submission_id=submission_id, run_id=run_id)

    @staticmethod
    def from_row(row):
        return RunJobModel(*row[1:])

    def format(
        self,
        pre,
        monochrome=False,
        brief=False,
        layer=0,
        njobs_in_parent=None,
        **kwargs,
    ):

        tb = "    "
        fin = self.finish if self.finish else DateUtil.now()
        time = round(DateUtil.secs_difference(self.start, fin)) if self.start else None
        # percentage = (
        #     (round(1000 * time / self.supertime) / 10)
        #     if (self.start and self.supertime)
        #     else None
        # )
        status = self.status or (
            sorted(self.events, key=lambda e: e.timestamp)[-1].status
            if self.events
            else TaskStatus.PROCESSING
        )

        name = self.name
        opts = []
        if self.shard is not None and self.shard >= 0:
            opts.append(f"shard-{self.shard}")
        if self.attempt and self.attempt > 1:
            opts.append(f"attempt-{self.attempt}")
        if len(opts) > 0:
            name += f" ({', '.join(opts)})"

        standard = pre + f"[{status.symbol()}] {name} ({second_formatter(time)})"

        col = ""
        uncol = ""

        if not monochrome:
            if status == TaskStatus.FAILED:
                col = _bcolors.FAIL
            elif status == TaskStatus.COMPLETED:
                col = _bcolors.OKGREEN
            # else:
            # col = _bcolors.UNDERLINE
            uncol = _bcolors.ENDC

        if (
            status != TaskStatus.COMPLETED
            or brief is False
            or (layer == 0 and njobs_in_parent == 1)
        ):
            if self.jobs:
                ppre = pre + tb
                subs: List[RunJobModel] = sorted(
                    self.jobs,
                    key=lambda j: (
                        j.shard or 0,
                        j.start if j.start else DateUtil.now(),
                    ),
                    reverse=False,
                )

                return (
                    col
                    + standard
                    + "".join(
                        [
                            "\n"
                            + j.format(
                                ppre, monochrome, brief, layer=layer + 1, **kwargs
                            )
                            for j in subs
                        ]
                    )
                    + uncol
                )

        fields: List[Tuple[str, str]] = []

        if status == TaskStatus.COMPLETED:
            if not self.finish:
                raise Exception(f"Finish was null for completed task: {self.name}")
            if self.cached:
                fields.append(("from cache", str(self.cached)))

        elif status == TaskStatus.RUNNING:
            fields.extend([("batchid", self.batchid), ("backend", self.backend)])

        elif status == TaskStatus.FAILED:
            fields.extend([("stdout", self.stdout), ("stderr", self.stderr)])
        elif status == TaskStatus.PROCESSING:
            pass
        elif status == TaskStatus.QUEUED:
            pass

        else:
            return (
                standard
                + f" :: Unimplemented status: '{status}' for task: '{self.name}'"
            )

        ppre = "\n" + " " * len(pre) + 2 * tb
        retval = standard + "".join(f"{ppre}{f[0]}: {f[1]}" for f in fields if f[1])

        return col + retval + uncol


class WorkflowJobEventModel:
    def __init__(self, jid: str, status: TaskStatus, timestamp: str):
        self.jid = jid
        self.status = status
        self.timestamp = timestamp
        if timestamp and isinstance(timestamp, str):
            self.timestamp = DateUtil.parse_iso(timestamp)

    @staticmethod
    def from_row(row):
        return RunJobModel(*row)
