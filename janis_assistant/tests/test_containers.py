import unittest
from typing import List

from janis_assistant.data.container.info import ContainerInfo


class TestOptionParsing(unittest.TestCase):
    def test_ubuntu(self):
        c = ContainerInfo.parse("ubuntu")
        self.assertIsNone(c.host)
        self.assertIsNone(c.repository)
        self.assertEqual("ubuntu", c.image)
        self.assertEqual("latest", c.tag)

    def test_python_3_8_1(self):
        c = ContainerInfo.parse("python:3.8.1")
        self.assertIsNone(c.host)
        self.assertIsNone(c.repository)
        self.assertEqual("python", c.image)
        self.assertEqual("3.8.1", c.tag)

    def test_library_python_3_8_1(self):
        c = ContainerInfo.parse("library/python:3.8.1")
        self.assertIsNone(c.host)
        self.assertEqual("library", c.repository)
        self.assertEqual("python", c.image)
        self.assertEqual("3.8.1", c.tag)

    def test_michaelfranklin_pmacutils_0_0_8(self):
        c = ContainerInfo.parse("michaelfranklin/pmacutil:0.0.6")
        self.assertIsNone(c.host)
        self.assertEqual("michaelfranklin", c.repository)
        self.assertEqual("pmacutil", c.image)
        self.assertEqual("0.0.6", c.tag)

    def test_quay_biocontainers_staden_io_lib_1_14_12(self):
        c = ContainerInfo.parse(
            "quay.io/biocontainers/staden_io_lib:1.14.12--h244ad75_0"
        )
        self.assertEqual("quay.io", c.host)
        self.assertEqual("biocontainers", c.repository)
        self.assertEqual("staden_io_lib", c.image)
        self.assertEqual("1.14.12--h244ad75_0", c.tag)
