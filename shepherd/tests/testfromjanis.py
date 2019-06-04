from unittest import TestCase

from janis import CaptureType
from janis.examples.simple import SimpleWorkflow
from shepherd.data.models.filescheme import GCSFileScheme

from shepherd.engines import Cromwell

from shepherd.management.configmanager import ConfigManager

from shepherd.management.taskmanager import TaskManager
from shepherd.environments.environment import Environment
from shepherd.validation import generate_validation_workflow_from_janis, ValidationRequirements

from shepherd.main import fromjanis

class TestFromJanisValidator(TestCase):

    def test_validate_from_janis_workflow_generation(self):
        reqs = ValidationRequirements("truth.vcf", "reference.fasta", ["out"])
        w = generate_validation_workflow_from_janis(SimpleWorkflow(), reqs)
        w.translate("wdl")

    def test_from_janis_run(self):
        wf = SimpleWorkflow()
        wf._inputs[0].input.value = "/Users/franklinmichael/Desktop/workflows-for-testing/03-simple/inputs/hello.tar"
        TaskManager.from_janis(wf, Environment.get_predefined_environment_by_id("local"), None)


class TestFromJanisIntegration(TestCase):

    def test_whole_genome_germline(self):
        file = "wg-germline-30x.py"

        fromjanis(
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
        tid = "e5ee1a"  # "ab23cc"
        ConfigManager().from_tid(tid).resume_if_possible()
