import unittest
from typing import List

from janis_assistant.templates import (
    validate_template_params,
    get_schema_for_template,
    EnvironmentTemplate,
)


class TestOptionParsing(unittest.TestCase):
    def test_option_parsing_one_correct(self):
        class my_function(EnvironmentTemplate):
            def __init__(self, val1):
                super().__init__(None, None)

        options = {"val1": "test"}
        self.assertTrue(validate_template_params(my_function, options))

    def test_option_parsing_two_correct(self):
        class my_function(EnvironmentTemplate):
            def __init__(self, val1, val2="test"):
                super().__init__(None, None)

        options = {"val1": "test", "val2": "test"}
        self.assertTrue(validate_template_params(my_function, options))

    def test_option_parsing_missing_required(self):
        class my_function(EnvironmentTemplate):
            def __init__(self, val1):
                super().__init__(None, None)

        options = {}
        self.assertRaises(
            Exception, validate_template_params, template=my_function, options=options
        )

    def test_option_parsing_default_one_param(self):
        class my_function(EnvironmentTemplate):
            def __init__(self, val1, val2="test"):
                super().__init__(None, None)

        options = {"val1": True}
        self.assertTrue(validate_template_params(my_function, options))

    def test_option_parsing_one_extra(self):
        class my_function(EnvironmentTemplate):
            def __init__(self, val1, val2="test"):
                super().__init__(None, None)

        options = {"val1": True, "unrecognised": False}
        self.assertRaises(
            Exception, validate_template_params, teplate=my_function, options=options
        )


class TestTemplateSchemaGeneration(unittest.TestCase):
    class TestTemplate(EnvironmentTemplate):
        def __init__(
            self, queues: List[str] = None, containerDir: str = "/default/dir"
        ):
            super().__init__(None, None)

    @classmethod
    def setUpClass(cls):
        cls.inputs = get_schema_for_template(TestTemplateSchemaGeneration.TestTemplate)

    def test_general_parse(self):
        self.assertEqual(2, len(self.inputs))

    def test_queues(self):
        inp = self.inputs[0]

        self.assertEqual("queues", inp.identifier)
        self.assertEqual(List[str], inp.type)
        self.assertTrue(inp.optional)
        self.assertIsNone(inp.default)

    def test_container_dir(self):
        inp = self.inputs[1]

        self.assertEqual("containerDir", inp.identifier)
        self.assertEqual(str, inp.type)
        self.assertTrue(inp.optional)
        self.assertEqual("/default/dir", inp.default)
