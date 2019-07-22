import unittest

from janis_runner.utils import write_files_into_buffered_zip


class TestZip(unittest.TestCase):
    def test_zip_dependencies(self):

        res = write_files_into_buffered_zip(TestZip.files_to_zip)
        self.assertTrue(True, msg="Didn't make it through without crashing :(")

    files_to_zip = [
        ("hello.txt", "Hello, World"),
        ("nested/file.txt", "This is a nested file"),
    ]
