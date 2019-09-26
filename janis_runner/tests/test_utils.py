from unittest import TestCase
from janis_runner.utils import (
    get_extension,
    second_formatter,
    parse_additional_arguments,
    convert_prefix_to_argname,
    try_parse_primitive_type,
)


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


class TestGetTimeFormat(TestCase):
    def test_under_minute_1(self):
        secs = 0
        self.assertEqual("0", second_formatter(secs))

    def test_under_minute_2(self):
        secs = 8
        self.assertEqual("8", second_formatter(secs))

    def test_under_minute_3(self):
        secs = 22
        self.assertEqual("22", second_formatter(secs))

    def test_under_minute_4(self):
        secs = 59
        self.assertEqual("59", second_formatter(secs))

    def test_under_hour_1(self):
        secs = 60
        self.assertEqual("1:00", second_formatter(secs))

    def test_under_hour_2(self):
        secs = 70
        self.assertEqual("1:10", second_formatter(secs))

    def test_under_hour_3(self):
        secs = 609
        self.assertEqual("10:09", second_formatter(secs))

    def test_under_hour_4(self):
        secs = 3599
        self.assertEqual("59:59", second_formatter(secs))

    def test_under_day_1(self):
        secs = 3600
        self.assertEqual("1:00:00", second_formatter(secs))

    def test_under_day_2(self):
        secs = 3660
        self.assertEqual("1:01:00", second_formatter(secs))

    def test_under_day_3(self):
        secs = 86399
        self.assertEqual("23:59:59", second_formatter(secs))

    def test_day(self):
        secs = 86400
        self.assertEqual("1:00:00:00", second_formatter(secs))


class TestSimplePrefixConverter(TestCase):
    def test_simple_prefix(self):
        self.assertEqual("test", convert_prefix_to_argname("--test"))

    def test_separated_prefix(self):
        self.assertEqual("test_case", convert_prefix_to_argname("--test-case"))


class TestSimpleParseArg(TestCase):
    def test_parse_simple_true(self):
        self.assertTrue(try_parse_primitive_type("true"))
        self.assertTrue(try_parse_primitive_type("TRUE"))
        self.assertTrue(try_parse_primitive_type("True"))
        self.assertTrue(try_parse_primitive_type("tuRe"))

    def test_parse_simple_false(self):
        self.assertFalse(try_parse_primitive_type("false"))
        self.assertFalse(try_parse_primitive_type("FALSE"))
        self.assertFalse(try_parse_primitive_type("False"))
        self.assertFalse(try_parse_primitive_type("fAlSe"))

    def test_parse_simple_int_1(self):
        i = try_parse_primitive_type("1")
        self.assertIsInstance(i, int)
        self.assertEqual(1, i)

    def test_parse_simple_int_2(self):
        i = try_parse_primitive_type("12345")
        self.assertIsInstance(i, int)
        self.assertEqual(12345, i)

    def test_parse_simple_int_3(self):
        i = try_parse_primitive_type("-1")
        self.assertIsInstance(i, int)
        self.assertEqual(-1, i)

    def test_parse_simple_float_1(self):
        i = try_parse_primitive_type("1.0")
        self.assertIsInstance(i, float)
        self.assertEqual(1.0, i)

    def test_parse_simple_float_2(self):
        i = try_parse_primitive_type("123.45")
        self.assertIsInstance(i, float)
        self.assertEqual(123.45, i)

    def test_parse_simple_float_3(self):
        i = try_parse_primitive_type("-1.0")
        self.assertIsInstance(i, float)
        self.assertEqual(-1.0, i)


class TestSimpleArgParser(TestCase):
    def test_one_flag(self):
        self.assertDictEqual(
            {"sampleflag": True}, parse_additional_arguments(["--sampleflag"])
        )

    def test_two_flags(self):
        self.assertDictEqual(
            {"flag1": True, "flag2": True},
            parse_additional_arguments(["--flag1", "--flag2"]),
        )

    def test_parse_prefix_and_value(self):
        self.assertDictEqual(
            {"myprefix": "value"}, parse_additional_arguments(["--myprefix", "value"])
        )

    def test_parse_two_prefixes_and_value(self):
        self.assertDictEqual(
            {"myprefix1": "value1", "myprefix2": "value2"},
            parse_additional_arguments(
                ["--myprefix1", "value1", "--myprefix2", "value2"]
            ),
        )

    def test_parse_two_values(self):
        self.assertDictEqual(
            {"myprefix": ["value1", "value2"]},
            parse_additional_arguments(["--myprefix", "value1", "value2"]),
        )

    def test_parse_three_values(self):
        self.assertDictEqual(
            {"myprefix": ["val1", "val2", "val3"]},
            parse_additional_arguments(["--myprefix", "val1", "val2", "val3"]),
        )

    def test_parse_multiple_mixed_values(self):
        self.assertDictEqual(
            {"my_mixed_bag": [4.7, True, "valueish", 0]},
            parse_additional_arguments(
                ["--my-mixed-bag", "4.7", "true", "valueish", "0"]
            ),
        )
