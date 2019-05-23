from unittest import TestCase
from janis.examples.simple import SimpleWorkflow

from shepherd.management.taskmanager import TaskManager
from shepherd.environments.environment import Environment
from shepherd.validation import generate_validation_workflow_from_janis, ValidationRequirements

from shepherd.main import main

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
        file = "wg-germline.py"
        main(
            file,
            ValidationRequirements(truthVCF="/Users/franklinmichael/Desktop/variants/gold.vcf",
                intervals="/Users/franklinmichael/Desktop/variants/BRCA1.bed",
                reference="/Users/franklinmichael/reference/hg38/assembly_contigs_renamed/Homo_sapiens_assembly38.fasta",
                fields=["variants_gatk", "variants_strelka", "variants_vardict", "combinedVariants"]
            ),
            env="local-connect"
        )

    def test_reconnect(self):
        tid = "d92ca2"
        TaskManager.from_tid(tid)
