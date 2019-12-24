from datetime import datetime
from typing import Optional, Union, List, Tuple

from janis_assistant.data.enums.taskstatus import TaskStatus
from janis_assistant.utils import second_formatter
from janis_assistant.utils.dateutil import DateUtil
from janis_core.utils.logger import _bcolors


class WorkflowJobModel:
    def __init__(
        self,
        jid: str,
        parentjid: Optional[str],
        name: str,
        batchid: Optional[str],
        shard: Optional[int],
        attempt: Optional[int],
        container: Optional[str],
        status: TaskStatus,
        start: Union[str, datetime],
        finish: Optional[Union[str, datetime]],
        backend: Optional[str],
        cached: bool,
        stdout: Optional[str],
        stderr: Optional[str],
        jobs: Optional[list] = None,
    ):
        self.jid = jid
        self.parentjid = parentjid
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

        self.jobs = jobs or None
        self.events = None

    @staticmethod
    def from_row(row):
        return WorkflowJobModel(*row[1:])

    def format(self, pre):

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
        if self.shard is not None and self.shard >= 0:
            name += f"_shard-{self.shard}"
        if self.attempt and self.attempt > 1:
            name += f"_attempt-{self.attempt}"

        standard = pre + f"[{status.symbol()}] {name} ({second_formatter(time)})"

        col = ""

        if status == TaskStatus.FAILED:
            col = _bcolors.FAIL
        elif status == TaskStatus.COMPLETED:
            col = _bcolors.OKGREEN
        # else:
        # col = _bcolors.UNDERLINE

        if self.jobs:
            ppre = pre + tb
            subs: List[WorkflowJobModel] = sorted(
                self.jobs,
                key=lambda j: j.start if j.start else DateUtil.now(),
                reverse=False,
            )

            return (
                col
                + standard
                + "".join(["\n" + j.format(ppre) for j in subs])
                + _bcolors.ENDC
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

        return col + retval + _bcolors.ENDC


class WorkflowJobEventModel:
    def __init__(self, jid: str, status: TaskStatus, timestamp: str):
        self.jid = jid
        self.status = status
        self.timestamp = timestamp
        if timestamp and isinstance(timestamp, str):
            self.timestamp = DateUtil.parse_iso(timestamp)

    @staticmethod
    def from_row(row):
        return WorkflowJobModel(*row)
