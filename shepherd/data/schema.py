from enum import Enum
from datetime import datetime
from typing import List, Optional, Tuple

from shepherd.utils import second_formatter


class TaskStatus(Enum):
    PROCESSING = 0
    QUEUED = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    TERMINATED = 5

    @staticmethod
    def FINAL_STATES(): return [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TERMINATED]

    def __str__(self):
        __str = {
            TaskStatus.PROCESSING.value: "Processing",
            TaskStatus.QUEUED.value: "Queued",
            TaskStatus.RUNNING.value: "Running",
            TaskStatus.COMPLETED.value: "Completed",
            TaskStatus.FAILED.value: "Failed",
            TaskStatus.TERMINATED.value: "Terminated"
        }
        return __str[self.value]

    def symbol(self):
        __str = {
            TaskStatus.PROCESSING.value: "...",
            TaskStatus.QUEUED.value: " ",
            TaskStatus.RUNNING.value: "~",
            TaskStatus.COMPLETED.value: "x",
            TaskStatus.FAILED.value: "!",
            TaskStatus.TERMINATED.value: "T"
        }
        return __str[self.value]


class TaskMetadata:

    def __init__(self, wid: str, name: str, status: TaskStatus, start: datetime, finish: Optional[datetime],
                 outputs: List, jobs: List, error: Optional[str]):
        self.tid = None             # needs to be set by taskManager
        self.outdir: str = None     # provided by TaskManager

        self.engine_name = None
        self.engine_url = None

        self.wid: str = str(wid)
        self.name: str = name
        self.status: TaskStatus = status
        self.start: datetime = start
        self.finish: datetime = finish
        self.outputs = outputs
        self.error = error

        self.jobs: List[JobMetadata] = jobs

    def format(self):
        tb = "    "
        nl = "\n"

        fin = self.finish if self.finish else datetime.now()
        duration = round((fin.replace(tzinfo=None) - self.start.replace(tzinfo=None)).total_seconds()) if self.start else 0

        return f"""
TID:        {self.tid}
WID:        {self.wid}
Name:       {self.name}
{(("Engine:     " + self.engine_name) if self.engine_name else '')}{("Egn url:    " + self.engine_url) if self.engine_url else ''}

Path:       {self.outdir}

Status:     {self.status}
Duration:   {second_formatter(duration)}
Start:      {self.start.isoformat() if self.start else 'N/A'}
Finish:     {self.finish.isoformat() if self.finish else "N/A"}

Jobs: 
{nl.join(j.format(tb) for j in sorted(self.jobs, key=lambda j: j.start))}       

{("Outputs:" + nl.join(tb + o for o in self.outputs)) if self.outputs else ''}
{("Error: " + self.error) if self.error else ''}
        """.strip()


class JobMetadata:
    def __init__(self, name: str, status: TaskStatus, job_id: Optional[str], backend: Optional[str],
                 runtime_attributes: Optional[dict], outputs: List, exec_dir: Optional[str], stdout: Optional[str],
                 stderr: Optional[str], start: datetime, finish: Optional[datetime], subjobs, from_cache: bool,
                 shard: Optional[int], super_time: Optional[int]):

        self.name = name
        self.status = status
        self.jobid = job_id
        self.shard = shard if shard is not None and shard >= 0 else None
        self.backend = backend
        self.runtimeattributes = runtime_attributes

        self.subjobs: List[JobMetadata] = subjobs
        self.fromcache = from_cache

        self.start: datetime = start
        self.finish: datetime = finish
        self.supertime = super_time

        self.outputs = outputs

        self.executiondir = exec_dir
        self.stdout = stdout
        self.stderr = stderr

    def format(self, pre):

        tb = "    "
        fin = self.finish if self.finish else datetime.now()
        time = round((fin.replace(tzinfo=None) - self.start.replace(tzinfo=None)).total_seconds()) if self.start else "N/A "
        percentage = (round(1000 * time / self.supertime)/10) if self.start else None

        shard = f"-shard-{self.shard}" if self.shard is not None else ""
        standard = pre + f"[{self.status.symbol()}] {self.name}{shard} ({time}s :: {percentage} %)"

        if self.subjobs:
            ppre = pre + tb
            subs: List[JobMetadata] = sorted(self.subjobs if self.subjobs else [], key=lambda j: j.start, reverse=False)

            return standard + "".join(["\n" + j.format(ppre) for j in subs])

        fields: List[Tuple[str, str]] = []

        if self.status == TaskStatus.COMPLETED:
            if not self.finish: raise Exception(f"Finish was null for completed task: {self.name}")
            if self.fromcache:
                fields.append(("from cache", str(self.fromcache)))

        elif self.status == TaskStatus.RUNNING:
            fields.extend([
                ("jid", self.jobid),
                ("backend", self.backend)
            ])

        elif self.status == TaskStatus.FAILED:
            fields.extend([
                ("stdout", self.stdout),
                ("stderr", self.stderr),
            ])
        elif self.status == TaskStatus.PROCESSING:
            pass
        elif self.status == TaskStatus.QUEUED:
            pass

        else:
            return standard + f" :: Unimplemented status: '{self.status}' for task: '{self.name}'"

        ppre = "\n" + " " * len(pre) + 2 * tb
        return standard + "".join(ppre + f[0] + ": " + f[1] for f in fields if f[1])



# if __name__ == "__main__":
#     jobs = [
#         JobMetadata("task1", TaskStatus.COMPLETED, start=datetime(2019,5,15,11,23), finish=datetime(2019, 5, 15, 11, 24),
#                     job_id=None, backend="local",  runtime_attributes={}, outputs=[],
#                     exec_dir=None, stdout=None, stderr=None),
#         JobMetadata("subworkflow", TaskStatus.RUNNING, start=datetime(2019, 5, 15, 11, 24), finish=datetime.now(),
#                     job_id=None, backend="local", runtime_attributes={}, outputs=[],
#                     exec_dir=None, stdout=None, stderr=None, subjobs=[
#                 JobMetadata("subtask1", TaskStatus.COMPLETED, start=datetime(2019, 5, 15, 11, 24),
#                             finish=datetime(2019, 5, 15, 11, 24, 30),
#                             job_id=None, backend="local", runtime_attributes={}, outputs=[],
#                             exec_dir=None, stdout=None, stderr=None),
#                 JobMetadata("subtask2", TaskStatus.RUNNING, start=datetime(2019, 5, 15, 11, 24, 31),
#                             finish=None,
#                             job_id="15462", backend="local", runtime_attributes={}, outputs=[],
#                             exec_dir=None, stdout=None, stderr=None)
#             ]),
#         JobMetadata("task3", TaskStatus.FAILED, start=datetime(2019, 5, 15, 11, 23, 1),
#                     finish=None,
#                     job_id="15462", backend="local", runtime_attributes={}, outputs=[],
#                     exec_dir=None, stdout=None, stderr=None)
#     ]
#     meta = TaskMetadata("1828", "whole_genome_germline", TaskStatus.RUNNING, datetime.now(), None, [], jobs=jobs)
#     print(meta.log())
