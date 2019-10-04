from datetime import datetime
from typing import Optional, Union, List

from janis_runner.data.models.workflowjob import WorkflowJobModel
from janis_runner.utils import second_formatter

from janis_runner.data.enums.taskstatus import TaskStatus
from janis_runner.utils.dateutil import DateUtil
from janis_runner.data.enums.workflowmetadatakeys import WorkflowMetadataDbKeys


class WorkflowModel:
    def __init__(
        self,
        wid: str = None,
        engine_wid: str = None,
        name: str = None,
        start: Union[str, datetime] = None,
        finish: Union[str, datetime] = None,
        execution_dir: str = None,
        status: TaskStatus = None,
        engine: str = None,
        engine_url: str = None,
        filesystem: str = None,
        labels: List[str] = None,
        error: str = None,
        author: str = None,
        jobs: List[WorkflowJobModel] = None,
    ):
        self.wid = wid
        self.engine_wid = engine_wid

        self.name = name

        self.start = start
        self.finish = finish
        self.execution_dir = execution_dir
        self.status = status

        self.engine = engine
        self.engine_url = engine_url
        self.filesystem = filesystem
        self.labels = labels

        self.error = error

        self.jobs: List[WorkflowJobModel] = jobs or []
        self.outputs = None

        self.author = author

    def format(self):
        tb = "    "
        nl = "\n"

        fin = self.finish if self.finish else DateUtil.now()
        duration = round((fin - self.start).total_seconds()) if self.start else 0

        return f"""\
WID:        {self.wid}
EngId:      {self.wid}
Name:       {self.name}
{((f"{nl}Engine:     " + self.engine) if self.engine else '')}{(
                    f"{nl}Engine url: " + self.engine_url) if self.engine_url else ''}

Task Dir:   {""}
Exec Dir:   {self.execution_dir}

Status:     {self.status}
Duration:   {second_formatter(duration)}
Start:      {self.start.isoformat() if self.start else 'N/A'}
Finish:     {self.finish.isoformat() if self.finish else "N/A"}

Jobs: 
{nl.join(j.format(tb) for j in sorted(self.jobs, key=lambda j: j.start))}       

{("Outputs:" + nl.join(tb + o for o in self.outputs)) if self.outputs else ''}
{("Error: " + self.error) if self.error else ''}
        """.strip()
