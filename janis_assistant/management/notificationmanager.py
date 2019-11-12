from typing import List
import subprocess

from janis_core import Logger

from janis_assistant.data.models.workflow import WorkflowModel
from janis_assistant.management.configuration import JanisConfiguration


class NotificationManager:
    @staticmethod
    def notify_status_change(status, metadata: WorkflowModel):

        body = NotificationManager._status_change_template.format(
            wid=metadata.wid,
            wfname=metadata.name,
            status=status,
            exdir=metadata.execution_dir,
            tdir=metadata.outdir,
        )

        NotificationManager.send_email(
            subject=f"{metadata.wid} status to {status}", body=body
        )
        return body

    @staticmethod
    def send_email(subject: str, body: str):

        nots = JanisConfiguration.manager().notifications

        mail_program = nots.mail_program

        if not mail_program:
            return Logger.log("Skipping email send as no mail program is configured")

        if not nots.email or nots.email.lower() == "none":
            Logger.log("Skipping notify status change as no email")
            return

        emails: List[str] = nots.email if isinstance(
            nots.email, list
        ) else nots.email.split(",")

        email_template = f"""\
Content-Type: text/html
To: {"; ".join(emails)}
From: janis-noreply@petermac.org
Subject: {subject}

{body}"""

        command = f"echo '{email_template}' | {mail_program}"
        Logger.log("Sending email with command: " + str(command.replace("\n", "\\n")))
        try:
            subprocess.call(command, shell=True)
        except Exception as e:
            Logger.critical(f"Couldn't send email '{subject}' to {emails}: {e}")

    _status_change_template = """\
<h1>Status change: {status}</h1>
    
<p>
    The workflow '{wfname}' ({wid}) moved to the '{status}' status.
</p>
<ul>
    <li>Task directory: <code>{tdir}</code></li>
    <li>Execution directory: <code>{exdir}</code></li>
</ul>
    
<p>Kind regards, <br />Janis</p>"""
