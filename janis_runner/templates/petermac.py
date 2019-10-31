from typing import Union, List
from janis_runner.engines.enginetypes import EngineType
from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_runner.templates.base import EnvironmentTemplate
from janis_runner.templates.slurm import SlurmSingularityTemplate


class PeterMacTemplate(SlurmSingularityTemplate):

    # default_recipes = {
    #     "hg38": {
    #         "reference": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.fasta",
    #         "snps_dbsnp": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.dbsnp138.vcf.gz",
    #         "snps_1000gp": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/1000G_phase1.snps.high_confidence.hg38.vcf.gz",
    #         "known_indels": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.known_indels.vcf.gz",
    #         "mills_indels": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz",
    #     }
    # }

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
            && image={self.container_dir}/$docker_subbed.sif && singularity pull $image docker://${{docker}}'"

        super().__init__(
            mail_program="sendmail -t",
            containerDir=containerDir,
            executionDir=executionDir,
            queues=queues,
            email=email,
            catchSlurmErrors=catchSlurmErrors,
            buildInstructions=singbuild,
            singularityLoadInstructions=singload,
        )

    def engine_config(self, engine: EngineType):
        if engine == EngineType.cromwell:
            return self.cromwell()

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
