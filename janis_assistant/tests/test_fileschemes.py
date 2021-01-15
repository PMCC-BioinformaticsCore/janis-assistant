import os
import tempfile
from shutil import rmtree
import unittest

from janis_assistant.management.filescheme import SSHFileScheme, GCSFileScheme


@unittest.skipUnless(
    os.getenv("unittest_ssh_location") is not None,
    "These tests only work with pretty specific environment setup",
)
class TestSSHFileScheme(unittest.TestCase):
    """
    These tests will only work on mfranklin's computer, and only when there's an ssh shortcut called fs.

    It also relies that a folder called 'shepherd-ssh-tests' exists in the home (~/) directory,
    and a file callled 'sample.txt' exists within that folder

    """

    def setUp(self):
        self.tmpdir = tempfile.gettempdir()
        self.path = self.tmpdir + "test-ssh" + "/"
        if os.path.exists(self.path):
            rmtree(self.path)
        os.makedirs(self.path)

        self.fs = SSHFileScheme("ssh-filescheme", os.getenv("unittest_ssh_location"))
        self.fs.makedir("~/shepherd-ssh-tests")

    def test_copy_ssh_to_and_from(self):
        local_path = self.path + "hello.txt"
        local2_path = self.path + "hello2.txt"
        spartan_path = "~/shepherd-ssh-tests/hello.txt"

        contents = "test_copy_ssh_spartan_to"
        with open(local_path, "w+") as r:
            r.write(contents)

        self.fs.cp_to(local_path, spartan_path, None)
        # check if it's copied by copying back to somwehere different
        self.fs.cp_from(spartan_path, local2_path, None)
        self.assertTrue(os.path.exists(local2_path))
        with open(local2_path) as f:
            self.assertEqual(contents, f.readline())


class TestGCSFileScheme(unittest.TestCase):
    def test_parsing_1(self):
        uri = "gs://genomics-public-data/references/hg38/v0/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi"
        bucket, blob = GCSFileScheme.parse_gcs_link(uri)

        self.assertEqual("genomics-public-data", bucket)
        self.assertEqual(
            "references/hg38/v0/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi", blob
        )
