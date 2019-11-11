from typing import Union, List

from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.slurm import SlurmSingularityTemplate


class PeterMacTemplate(SlurmSingularityTemplate):
    def __init__(
        self,
        executionDir: str,
        queues: Union[str, List[str]] = "prod_med,prod",
        email=None,
        containerDir="/config/binaries/singularity/containers_devel/janis/",
        singularityVersion="3.4.0",
        catchSlurmErrors=False,
    ):

        singload = "module load singularity"
        if singularityVersion:
            singload += "/" + str(singularityVersion)

        joined_queued = ",".join(queues) if isinstance(queues, list) else str(queues)

        # Very cromwell specific at the moment, need to generalise this later
        singbuild = f"sbatch -p {joined_queued} --wait \
--wrap 'unset SINGULARITY_TMPDIR && docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}}) \
&& image={containerDir}/$docker_subbed.sif && singularity pull $image docker://${{docker}}'"

        super().__init__(
            mail_program="sendmail -t",
            executionDir=executionDir,
            queues=joined_queued,
            email=email,
            catchSlurmErrors=catchSlurmErrors,
            buildInstructions=singbuild,
            singularityLoadInstructions=singload,
            containerDir=containerDir,
            limitResources=False,
        )

    def engine_config(self, engine: EngineType):
        if engine == EngineType.cromwell:
            return self.cromwell()

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
