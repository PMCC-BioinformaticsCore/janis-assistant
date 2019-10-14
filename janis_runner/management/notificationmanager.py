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

        email = nots.email if isinstance(nots.email, list) else nots.email.split(",")

        NotificationManager.send_email(
            email, subject=f"{metadata.wid} status to {status}", body=body
        )
        return body

    @staticmethod
    def send_email(to: List[str], subject: str, body: str):

        mail_program = JanisConfiguration.manager().template.template.mail_program

        if not mail_program:
            return Logger.log("Skipping email send as no mail program is configured")

        email_template = f"""\
Content-Type: text/html
To: {",".join(to)}
From: janis-noreply@petermac.org

{body}"""

        command = f"echo '{email_template}' > {mail_program}"
        Logger.log("Sending email with command: " + str(command))
        try:
            subprocess.call(command, shell=True)
        except Exception as e:
            Logger.critical(f"Couldn't send email '{subject}' to {to}: {e}")

    _status_change_template = """\
<h1>Status change: {status}</h1>
    
<p>
    The workflow '{wfname}' ({wid}) moved to the '{status}' status.
</p>
<ul>
    <li>Task directory: {tdir}</li>
    <li>Execution directory: {exdir}</li>
</ul>
    
<p>Kind regards, Janis</p>"""
