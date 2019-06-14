import unittest

from janis import CaptureType
from janis.examples.simple import SimpleWorkflow
from shepherd.data.models.filescheme import GCSFileScheme

from shepherd.engines import Cromwell

from shepherd.management.configmanager import ConfigManager

from shepherd.management.taskmanager import TaskManager
from shepherd.environments.environment import Environment
from shepherd.validation import generate_validation_workflow_from_janis, ValidationRequirements

from shepherd.main import fromjanis


class TestFromJanisValidator():

    def test_validate_from_janis_workflow_generation(self):
        reqs = ValidationRequirements("truth.vcf", "reference.fasta", ["out"])
        w = generate_validation_workflow_from_janis(SimpleWorkflow(), reqs)
        w.translate("wdl")

    def test_from_janis_run(self):
        wf = SimpleWorkflow()
        wf._inputs[0].input.value = "/Users/franklinmichael/Desktop/workflows-for-testing/03-simple/inputs/hello.tar"
        TaskManager.from_janis(wf, Environment.get_predefined_environment_by_id("local"), None)


class TestFromJanisIntegration(unittest.TestCase):

    def test_whole_genome_germline_gcp(self):
        file = "/Users/franklinmichael/janis-search-path/wg-germline-30x-gcp.py"

        print(fromjanis(
            file,
            validation_reqs=ValidationRequirements(
                truthVCF="gs://peter-mac-cromwell/reference/gold.vcf",
                intervals="gs://peter-mac-cromwell/reference/WGS_30X.bed",
                reference="gs://peter-mac-cromwell/reference/assembly_contigs_renamed/Homo_sapiens_assembly38.fasta",
                fields=["variants_gatk"]
            ),
            env="gcp",
            hints={CaptureType.key(): CaptureType.THIRTYX},
            # dryrun=True,
            inputs="/Users/franklinmichael/janis-search-path/wgs-30x-additional-inputs.json",
            watch=False
        ))

    def test_whole_genome_germline_pmac(self):
        file = "/Users/franklinmichael/janis-search-path/wg-germline-30x-pmac.py"

        print(fromjanis(
            file,
            validation_reqs=ValidationRequirements(
                truthVCF="/researchers/jiaan.yu/WGS_pipeline/germline/GIAB_NA12878/high_conf_calls/normalised_files/high_conf.norm.vcf",
                intervals="/researchers/jiaan.yu/WGS_pipeline/germline/GIAB_NA12878/test_cases/test2_WGS_30X/other_files/WGS_30X.bed",
                reference="/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.fasta",
                fields=["variants_gatk", "variants_vardict", "variants_strelka", "combinedVariants"]
            ),
            env="pmac",
            hints={CaptureType.key(): CaptureType.THIRTYX},
            # dryrun=True,
            # inputs="/Users/franklinmichael/janis-search-path/wgs-30x-additional-inputs.json",
            watch=False
        ))

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
        file = "/Users/franklinmichael/source/janis/janis/examples/simple.py"

        fromjanis(
            file,
            env="gcp",
            # env=Environment("local-connect-9001", Cromwell.from_url("local-9001", url="localhost:9001"),
            #                 filescheme=GCSFileScheme()),
            dryrun=True,
            hints=None,
            validation_reqs=None
        )

    def test_reconnect(self):
        tid = "fbe6ad" # 44395a"
        ConfigManager.manager().from_tid(tid).resume_if_possible()
