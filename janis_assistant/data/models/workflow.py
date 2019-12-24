from datetime import datetime
from datetime import datetime
from typing import Union, List

from janis_assistant.data.enums.taskstatus import TaskStatus
from janis_assistant.data.models.outputs import WorkflowOutputModel
from janis_assistant.data.models.workflowjob import WorkflowJobModel
from janis_assistant.utils import second_formatter
from janis_assistant.utils.dateutil import DateUtil


class WorkflowModel:
    def __init__(
        self,
        wid: str = None,
        engine_wid: str = None,
        name: str = None,
        start: Union[str, datetime] = None,
        finish: Union[str, datetime] = None,
        execution_dir: str = None,
        outdir: str = None,
        status: TaskStatus = None,
        engine: str = None,
        engine_url: str = None,
        filesystem: str = None,
        labels: List[str] = None,
        error: str = None,
        author: str = None,
        jobs: List[WorkflowJobModel] = None,
        last_updated: datetime = None,
        tags: List[str] = None,
        outputs: List[WorkflowOutputModel] = None,
    ):
        self.wid = wid
        self.engine_wid = engine_wid

        self.name = name

        self.start = start
        self.finish = finish
        self.execution_dir = execution_dir
        self.outdir = outdir
        self.status = status
        self.last_updated = last_updated

        self.engine = engine
        self.engine_url = engine_url
        self.filesystem = filesystem
        self.labels = labels
        self.tags = tags if tags else []

        self.error = error

        self.jobs: List[WorkflowJobModel] = jobs or []
        self.outputs: List[WorkflowOutputModel] = outputs

        self.author = author

    def format(self):
        tb = "    "
        nl = "\n"

        fin = self.finish if self.finish else DateUtil.now()
        duration = round((fin - self.start).total_seconds()) if self.start else 0

        updated_text = "Unknown"
        if self.last_updated:
            secs_ago = int((DateUtil.now() - self.last_updated).total_seconds())
            if secs_ago > 2:
                updated_text = second_formatter(secs_ago) + " ago"
            else:
                updated_text = "Just now"
            updated_text += f" ({self.last_updated.replace(microsecond=0).isoformat()})"

        return f"""\
WID:        {self.wid}
EngId:      {self.engine_wid}
Name:       {self.name}
Engine:     {self.engine}

Task Dir:   {self.outdir}
Exec Dir:   {self.execution_dir}

Status:     {self.status}
Duration:   {second_formatter(duration)}
Start:      {self.start.isoformat() if self.start else 'N/A'}
Finish:     {self.finish.isoformat() if self.finish else "N/A"}
Updated:    {updated_text}

Jobs: 
{nl.join(j.format(tb) for j in sorted(self.jobs, key=lambda j: j.start or DateUtil.now()))}       

{("Outputs:" + "".join(nl + tb + o.format() for o in self.outputs) if self.outputs else '')}
{("Error: " + self.error) if self.error else ''}
        """.strip()
