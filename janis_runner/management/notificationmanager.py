from typing import List
import subprocess

from janis_core import Logger

from janis_runner.data.models.workflow import WorkflowModel
from janis_runner.management.configuration import JanisConfiguration


class NotificationManager:
    @staticmethod
    def notify_status_change(status, metadata: WorkflowModel):

        nots = JanisConfiguration.manager().notifications

        if not nots.email:
            Logger.log("Skipping notify status change as no email")
            return

        body = NotificationManager._status_change_template.format(
            wid=metadata.wid,
            wfname=metadata.name,
            status=status,
            exdir=metadata.execution_dir,
            tdir=metadata.outdir,
        )
        NotificationManager.send_email(
            nots.email, subject=f"{metadata.wid} status to {status}", body=body
        )
        return body

    @staticmethod
    def send_email(to: List[str], subject: str, body: str):
        tos = ",".join(to)
        try:
            subprocess.call(
                ["echo", f"'{body}'", "|", "mailx", "-s", subject, tos], shell=True
            )
        except Exception as e:
            Logger.critical(f"Couldn't send email to {tos}: {e}")

    _status_change_template = """\
<h1>Status change: {status}</h1>
    
<p>
    The workflow '{wfname}' ({wid}) moved to the '{status}' status.
</p>
<ul>
    <li>Task directory: {tdir}</li>
    <li>Execution directory: {exdir}</li>
</ul>
    
<p>Kind regards, Janis</p>
    """
