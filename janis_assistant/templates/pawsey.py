import subprocess
from typing import Union, List

from janis_assistant.templates.slurm import SlurmSingularityTemplate
from janis_core import Logger

from janis_assistant.templates.petermac import PeterMacTemplate


class PawseyTemplate(SlurmSingularityTemplate):
    """
    https://support.pawsey.org.au/documentation/display/US/Queue+Policies+and+Limits

    Template for Pawsey. This submits Janis to the longq cluster. There is currently NO support
    for workflows that run for longer than 4 days, though workflows can be resubmitted after this
    job dies.

    It's proposed that Janis assistant could resubmit itself

    """

    SUBMISSION_LENGTH = "4-00:00:00"

    def __init__(
        self,
        executionDir: str,
        containerDir: str,
        queues: Union[str, List[str]] = "workq",
        submissionQueue: str = "longq",
        singularityVersion: str = "3.3.0",
        catchSlurmErrors=True,
        sendSlurmEmails=True,
        singularityBuildInstructions="singularity pull $image docker://${docker}",
        max_cores=28,
        max_ram=128,
    ):

        self.submission_queue = submissionQueue

        singload = "module load singularity"
        if singularityVersion:
            singload += "/" + str(singularityVersion)

        super().__init__(
            executionDir=executionDir,
            queues=queues,
            containerDir=containerDir,
            catchSlurmErrors=catchSlurmErrors,
            sendSlurmEmails=sendSlurmEmails,
            buildInstructions=singularityBuildInstructions,
            singularityLoadInstructions=singload,
            max_cores=max_cores,
            max_ram=max_ram,
        )


class PawseyDisconnectedTemplate(PawseyTemplate):
    def submit_detatched_resume(self, wid, command):
        q = self.queues
        jq = ", ".join(q) if isinstance(q, list) else q
        jc = " ".join(command) if isinstance(command, list) else command
        newcommand = [
            "sbatch",
            "-p",
            self.submission_queue or jq,
            "-J",
            f"janis-{wid}",
            "--time",
            self.SUBMISSION_LENGTH,
            "--wrap",
            jc,
        ]
        Logger.info("Starting command: " + str(newcommand))
        rc = subprocess.call(
            newcommand,
            close_fds=True,
            # stdout=subprocess.DEVNULL,
            # stderr=subprocess.DEVNULL,
        )
        if rc != 0:
            raise Exception(f"Couldn't submit janis-monitor, non-zero exit code ({rc})")
