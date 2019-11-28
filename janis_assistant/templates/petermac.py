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
        catchSlurmErrors=True,
        singularityBuildInstructions=None,
        max_cores=40,
        max_ram=256,
    ):
        """Peter Mac (login node) template

        Template to run Janis / Cromwell at the Peter MacCallum Cancer Centre (Rosalind)

        :param executionDir: Execution directory
        :param queues: The queue to submit jobs to
        :param containerDir: [OPTIONAL] Override the directory singularity containers are stored in
        :param singularityVersion: The version of Singularity to use on the cluster
        :param sendSlurmEmails: Send Slurm job notifications using the provided email
        :param catchSlurmErrors: Fail the task if Slurm kills the job (eg: memory / time)
        :param max_cores: Override maximum number of cores (default: 32)
        :param max_ram: Override maximum ram (default 508 [GB])
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
            max_cores=max_cores,
            max_ram=max_ram,
        )
