import os
import unittest

from janis_runner.utils import get_janis_workflow_from_searchname


class TestGetWorkflowFromFile(unittest.TestCase):
    # @unittest.skipUnless(os.getenv())
    def test_get_wgs_workflow(self):
        file = "/Users/franklinmichael/source/janis-examplepipelines/workflows/wholegenomegermline.py"

        ret = get_janis_workflow_from_searchname(file, "/Users/franklinmichael/")
        self.assertEqual("whole_genome_germline", ret().id())
