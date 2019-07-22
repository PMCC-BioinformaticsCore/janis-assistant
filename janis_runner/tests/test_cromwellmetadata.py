from unittest import TestCase

from janis_runner.engines.cromwell.metadata import CromwellMetadata


class TestCromwellMetadata(TestCase):
    def setUp(self):
        with open(
            "/Users/franklinmichael/Desktop/whole_genome_germline/wdl/workflow_testing/chr19/metadata-chr19-pmac-progress.json"
        ) as meta:
            self.exome_metadata = CromwellMetadata.parse(" ".join(meta.readlines()))

    # def test_call_time_stats(self):
    #     print(self.exome_metadata.generate_call_times())

    def test_metadata_conversion(self):
        print(self.exome_metadata.standard().format())
