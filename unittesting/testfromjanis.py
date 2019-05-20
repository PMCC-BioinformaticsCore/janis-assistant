from unittest import TestCase
from janis.examples.simple import SimpleWorkflow

from shepherd.validation import generate_validation_workflow_from_janis, ValidationRequirements


class TestFromJanisValidator(TestCase):

    def test_validate_from_janis_workflow_generation(self):
        reqs = ValidationRequirements("truth.vcf", "reference.fasta", ["out"])
        w = generate_validation_workflow_from_janis(SimpleWorkflow(), reqs)
        w.translate("wdl")
