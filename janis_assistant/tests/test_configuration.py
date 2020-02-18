import unittest

from janis_assistant.management.configuration import JanisConfiguration


class TestNoAttrErrors(unittest.TestCase):
    def test_1(self):
        jc = JanisConfiguration({})
        self.assertIsNone(jc.random_attribute)

    def test_cromwell(self):
        jc = JanisConfiguration({})
        self.assertIsNone(jc.cromwell.some_other_random_attribute)

    def test_get_state(self):
        jc = JanisConfiguration({})
        self.assertIsInstance(jc.__getstate__(), dict)
