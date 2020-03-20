import unittest

from janis_core import CaptureType

from janis_assistant.management.filescheme import LocalFileScheme
from janis_assistant.engines.toil.main import Toil
from janis_assistant.environments.environment import Environment
from janis_assistant.main import fromjanis
from janis_assistant.management.configmanager import ConfigManager
from janis_assistant.validation import ValidationRequirements


# class TestFromJanisValidator:
#     def test_validate_from_janis_workflow_generation(self):
#         reqs = ValidationRequirements("truth.vcf", "reference.fasta", ["out"])
#         w = generate_validation_workflow_from_janis(SimpleWorkflow(), reqs)


class TestFromJanisIntegration(unittest.TestCase):
    def test_whole_genome_germline_gcp(self):
        file = "/Users/franklinmichael/janis-search-path/wg-germline-30x-gcp.py"

        print(
            fromjanis(
                file,
                validation_reqs=ValidationRequirements(
                    truthVCF="gs://peter-mac-cromwell/reference/gold.vcf",
                    intervals="gs://peter-mac-cromwell/reference/WGS_30X.bed",
                    reference="gs://peter-mac-cromwell/reference/assembly_contigs_renamed/Homo_sapiens_assembly38.fasta",
                    fields=["variants_gatk"],
                ),
                env="gcp",
                hints={CaptureType.key(): CaptureType.THIRTYX},
                dryrun=True,
                inputs="/Users/franklinmichael/janis-search-path/wgs-30x-additional-inputs.json",
                watch=False,
            )
        )

    def test_whole_genome_germline_pmac(self):
        file = "/Users/franklinmichael/janis-search-path/wg-germline-30x-pmac.py"

        print(
            fromjanis(
                file,
                validation_reqs=ValidationRequirements(
                    truthVCF="/researchers/jiaan.yu/WGS_pipeline/germline/GIAB_NA12878/high_conf_calls/normalised_files/high_conf.norm.vcf",
                    intervals="/researchers/jiaan.yu/WGS_pipeline/germline/GIAB_NA12878/test_cases/test2_WGS_30X/other_files/WGS_30X.bed",
                    reference="/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.fasta",
                    fields=[
                        "variants_gatk",
                        "variants_vardict",
                        "variants_strelka",
                        "combinedVariants",
                    ],
                ),
                env="pmac",
                hints={CaptureType.key(): CaptureType.THIRTYX},
                dryrun=True,
                # inputs="/Users/franklinmichael/janis-search-path/wgs-30x-additional-inputs.json",
                watch=False,
            )
        )

    def test_somatic(self):
        file = "/Users/franklinmichael/source/janis-examplepipelines/workflows/somatic_pipeline.py"

        fromjanis(
            file,
            validation_reqs=None,
            # validation_reqs=ValidationRequirements(
            #     truthVCF="/researchers/jiaan.yu/WGS_pipeline/germline/GIAB_NA12878/high_conf_calls/normalised_files/high_conf.norm.vcf",
            #     intervals="/researchers/jiaan.yu/WGS_pipeline/germline/GIAB_NA12878/test_cases/test2_WGS_30X/other_files/WGS_30X.bed",
            #     reference="/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.fasta",
            #     fields=["variants_gatk", "variants_vardict", "variants_strelka", "combinedVariants"]
            # ),
            env="local-connect",
            hints={CaptureType.key(): CaptureType.TARGETED},
            # dryrun=True,
            # inputs="wgs-30x-additional-inputs.json"
        )

    def test_simple(self):
        file = "/Users/franklinmichael/source/janis/janis/examples/echo.py"

        engine = Toil("simple-test")
        env = Environment("toil-local", engine, LocalFileScheme())

        fromjanis(
            file,
            env=env,
            # dryrun=True,
            hints=None,
            validation_reqs=None,
        )

    def test_reconnect(self):
        wid = "fbe6ad"  # 44395a"
        ConfigManager.manager().from_tid(tid).resume()
