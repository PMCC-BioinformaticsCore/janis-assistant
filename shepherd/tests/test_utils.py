from unittest import TestCase
from shepherd.utils import get_extension


class TestGetExtension(TestCase):

    def test_regular(self):
        self.assertEqual("txt", get_extension("test.txt"))

    def test_double(self):
        self.assertEqual("txt", get_extension("test.intermediate.txt"))

    def test_triple(self):
        self.assertEqual("txt", get_extension("test.intermediate.filenames.txt"))

    def test_none(self):
        self.assertIsNone(get_extension("test"))

    def test_last_dot(self):
        self.assertIsNone(get_extension("test."))

    def test_first_dot(self):
        self.assertEqual("test", get_extension(".test"))
