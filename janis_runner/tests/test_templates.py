import unittest

from janis_runner.engines.cromwell.configtemplates import (
    from_template,
    template_kwarg_parser,
)


class TestOptionParsing(unittest.TestCase):
    def test_option_parsing_one_no_default(self):
        def my_function(val1):
            pass

        options = {"val1": "test"}
        self.assertDictEqual(options, template_kwarg_parser(my_function, options))

    def test_option_parsing_two_one_applied_default(self):
        def my_function(val1, val2="test"):
            pass

        options = {"val1": "test"}
        expected = {"val1": "test", "val2": "test"}
        self.assertDictEqual(expected, template_kwarg_parser(my_function, options))

    def test_option_parsing_two_one_overrided_default(self):
        def my_function(val1, val2="test"):
            pass

        options = {"val1": "test", "val2": "override"}
        self.assertDictEqual(options, template_kwarg_parser(my_function, options))
