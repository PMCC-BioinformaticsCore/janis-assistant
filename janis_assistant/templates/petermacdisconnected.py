import subprocess
from typing import Union, List

from janis_core import Logger

from janis_assistant.templates.petermac import PeterMacTemplate


class PeterMacDisconnectedTemplate(PeterMacTemplate):
    def __init__(
        self,
        executionDir: str,
        queues: Union[str, List[str]] = "prod_med,prod",
        containerDir="/config/binaries/singularity/containers_devel/janis/",
        singularityVersion="3.4.0",
        catchSlurmErrors=True,
    ):

        buildinstructions = (
            f"unset SINGULARITY_TMPDIR && docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}}) "
            f"&& image={containerDir}/$docker_subbed.sif && singularity pull $image docker://${{docker}}"
        )

        super().__init__(
            executionDir=executionDir,
            queues=queues,
            containerDir=containerDir,
            singularityVersion=singularityVersion,
            catchSlurmErrors=catchSlurmErrors,
            singularityBuildInstructions=buildinstructions,
        )

    def submit_detatched_resume(self, wid, command):
        q = self.queues or "prod_short"
        jq = ", ".join(q) if isinstance(q, list) else q
        jc = " ".join(command) if isinstance(command, list) else command
        newcommand = [
            "sbatch",
            "-p",
            jq,
            "-J",
            f"janis-{wid}",
            "--time",
            "30",
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
