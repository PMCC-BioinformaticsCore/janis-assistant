import os
import tempfile
from shutil import rmtree
from unittest import TestCase

from shepherd.data.filescheme import SSHFileScheme


class TestSSHFileScheme(TestCase):
    """
    These tests will only work on mfranklin's computer, and only when there's an ssh shortcut called spartan.

    It also relies that a folder called 'shepherd-ssh-tests' exists in the home (~/) directory,
    and a file callled 'sample.txt' exists within that folder

    """

    def setUp(self):
        self.tmpdir = tempfile.gettempdir()
        self.path = self.tmpdir + "test-ssh" + "/"
        if os.path.exists(self.path):
            rmtree(self.path)
        os.makedirs(self.path)

        self.spartan = SSHFileScheme("spartan-fs-id", "spartan")

    def test_copy_ssh_spartan_from(self):
        local_path = self.path + "sample.txt"
        self.spartan.cp_from("~/shepherd-ssh-tests/sample.txt", local_path, None)
        self.assertTrue(os.path.exists(local_path))
        with open(local_path) as f:
            self.assertEqual("Hello, Spartan!\n", f.readline())

    def test_copy_ssh_spartan_to(self):
        local_path = self.path + "hello.txt"
        local2_path = self.path + "hello2.txt"
        spartan_path = "~/shepherd-ssh-tests/hello.txt"

        contents = "test_copy_ssh_spartan_to"
        with open(local_path, "w+") as r:
            r.write(contents)

        self.spartan.cp_to(local_path, spartan_path, None)
        # check if it's copied by copying back to somwehere different
        self.spartan.cp_from(spartan_path, local2_path, None)
        self.assertTrue(os.path.exists(local2_path))
        with open(local2_path) as f:
            self.assertEqual(contents, f.readline())

