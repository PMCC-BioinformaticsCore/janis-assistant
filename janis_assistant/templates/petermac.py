from typing import Union, List

from janis_assistant.templates.slurm import SlurmSingularityTemplate


class PeterMacTemplate(SlurmSingularityTemplate):
    def __init__(
        self,
        executionDir: str,
        queues: Union[str, List[str]] = "prod_med,prod",
        containerDir="/config/binaries/singularity/containers_devel/janis/",
        singularityVersion="3.4.0",
        sendSlurmEmails=False,
        catchSlurmErrors=False,
        singularityBuildInstructions=None,
    ):
        """
        :param executionDir:
        :param queues:
        :param containerDir:
        :param singularityVersion:
        :param sendSlurmEmails:
        :param catchSlurmErrors:
        :param singularityBuildInstructions: Sensible default for PeterMac template
        """

        singload = "module load singularity"
        if singularityVersion:
            singload += "/" + str(singularityVersion)

        joined_queued = ",".join(queues) if isinstance(queues, list) else str(queues)

        # Very cromwell specific at the moment, need to generalise this later
        if not singularityBuildInstructions:
            singularityBuildInstructions = f"sbatch -p {joined_queued} --wait \
    --wrap 'unset SINGULARITY_TMPDIR && docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}}) \
    && image={containerDir}/$docker_subbed.sif && singularity pull $image docker://${{docker}}'"

        super().__init__(
            mail_program="sendmail -t",
            executionDir=executionDir,
            queues=joined_queued,
            sendSlurmEmails=sendSlurmEmails,
            catchSlurmErrors=catchSlurmErrors,
            buildInstructions=singularityBuildInstructions,
            singularityLoadInstructions=singload,
            containerDir=containerDir,
            limitResources=False,
        )
