import unittest
from typing import List

from janis_runner.templates import validate_template_params, get_schema_for_template


class TestOptionParsing(unittest.TestCase):
    def test_option_parsing_one_correct(self):
        def my_function(val1):
            pass

        options = {"val1": "test"}
        self.assertTrue(validate_template_params(my_function, options))

    def test_option_parsing_two_correct(self):
        def my_function(val1, val2="test"):
            pass

        options = {"val1": "test", "val2": "test"}
        self.assertTrue(validate_template_params(my_function, options))

    def test_option_parsing_missing_required(self):
        def my_function(val1):
            pass

        options = {}
        self.assertRaises(
            Exception, validate_template_params, template=my_function, options=options
        )

    def test_option_parsing_default_one_param(self):
        def my_function(val1, val2="test"):
            pass

        options = {"val1": True}
        self.assertTrue(validate_template_params(my_function, options))

    def test_option_parsing_one_extra(self):
        def my_function(val1, val2="test"):
            pass

        options = {"val1": True, "unrecognised": False}
        self.assertRaises(
            Exception, validate_template_params, teplate=my_function, options=options
        )


class TestTemplateSchemaGeneration(unittest.TestCase):
    @staticmethod
    def template(
        executionDir: str, queues: List[str] = None, containerDir: str = "/default/dir"
    ):
        pass

    @classmethod
    def setUpClass(cls):
        cls.inputs = get_schema_for_template(TestTemplateSchemaGeneration.template)

    def test_general_parse(self):
        self.assertEqual(3, len(self.inputs))

    def test_execution_dir(self):
        inp = self.inputs[0]

        self.assertEqual("executionDir", inp.identifier)
        self.assertEqual(str, inp.type)
        self.assertFalse(inp.optional)
        self.assertIsNone(inp.default)

    def test_queues(self):
        inp = self.inputs[1]

        self.assertEqual("queues", inp.identifier)
        self.assertEqual(List[str], inp.type)
        self.assertTrue(inp.optional)
        self.assertIsNone(inp.default)

    def test_container_dir(self):
        inp = self.inputs[2]

        self.assertEqual("containerDir", inp.identifier)
        self.assertEqual(str, inp.type)
        self.assertTrue(inp.optional)
        self.assertEqual("/default/dir", inp.default)
