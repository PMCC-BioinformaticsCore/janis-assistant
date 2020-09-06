from unittest import TestCase
from os.path import join as ospathjoin

from janis_assistant.data.models.base import (
    _prepare_for_serialization,
    prep_object_for_db,
)
from janis_assistant.utils import (
    get_extension,
    second_formatter,
    parse_additional_arguments,
    convert_prefix_to_argname,
    try_parse_primitive_type,
    recursively_join,
    fully_qualify_filename,
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
        self.assertEqual("0s", second_formatter(secs))

    def test_under_minute_2(self):
        secs = 8
        self.assertEqual("8s", second_formatter(secs))

    def test_under_minute_3(self):
        secs = 22
        self.assertEqual("22s", second_formatter(secs))

    def test_under_minute_4(self):
        secs = 59
        self.assertEqual("59s", second_formatter(secs))

    def test_under_hour_1(self):
        secs = 60
        self.assertEqual("1m:00s", second_formatter(secs))

    def test_under_hour_2(self):
        secs = 70
        self.assertEqual("1m:10s", second_formatter(secs))

    def test_under_hour_3(self):
        secs = 609
        self.assertEqual("10m:09s", second_formatter(secs))

    def test_under_hour_4(self):
        secs = 3599
        self.assertEqual("59m:59s", second_formatter(secs))

    def test_under_day_1(self):
        secs = 3600
        self.assertEqual("1h:00m:00s", second_formatter(secs))

    def test_under_day_2(self):
        secs = 3660
        self.assertEqual("1h:01m:00s", second_formatter(secs))

    def test_under_day_3(self):
        secs = 86399
        self.assertEqual("23h:59m:59s", second_formatter(secs))

    def test_day(self):
        secs = 86400
        self.assertEqual("1d:00h:00m:00s", second_formatter(secs))


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

    def test_specified_multiple(self):
        self.assertDictEqual(
            {"t": [1, 2]}, parse_additional_arguments(["-t", "1", "-t", "2"])
        )

    def test_specified_multiple_nargs(self):
        self.assertDictEqual(
            {"t": [[1, 2], ["a", "b"]]},
            parse_additional_arguments(["-t", "1", "2", "-t", "a", "b"]),
        )


class TestFullyQualityPath(TestCase):
    @classmethod
    def setUpClass(cls):
        from os import getcwd

        cls.cwd = getcwd()

    def test_relative(self):
        fn = "test/my.txt"
        self.assertEqual(ospathjoin(self.cwd, fn), fully_qualify_filename(fn))

    def test_dot_relative(self):
        fn = "my.txt"
        self.assertEqual(ospathjoin(self.cwd, fn), fully_qualify_filename("./" + fn))

    def test_user_expand(self):
        from os.path import expanduser

        fn = "~/my.txt"
        self.assertEqual(expanduser(fn), fully_qualify_filename(fn))

    def test_nonrelative(self):
        fn = "/test/my.txt"
        self.assertEqual(fn, fully_qualify_filename(fn))

    def test_ignore_https(self):
        fn = "https://janis.readthedocs.io"
        self.assertEqual(fn, fully_qualify_filename(fn))

    def test_ignore_http(self):
        fn = "http://janis.readthedocs.io"
        self.assertEqual(fn, fully_qualify_filename(fn))

    def test_ignore_s3(self):
        fn = "s3://janis/readthedocs/io.txt"
        self.assertEqual(fn, fully_qualify_filename(fn))

    def test_ignore_gcs(self):
        fn = "gcs://janis/readthedocs/io.txt"
        self.assertEqual(fn, fully_qualify_filename(fn))


class TestRecursiveJoin(TestCase):
    def test_empty(self):
        self.assertEqual("", recursively_join([], ","))

    def test_one_layer(self):
        ar = ["1", "2", "3"]
        self.assertEqual("1,2,3", recursively_join(ar, ","))

    def test_one_layer_none(self):
        ar = ["1", None, "3"]
        self.assertEqual("1,None,3", recursively_join(ar, ","))

    def test_one_layer_mixed(self):
        ar = ["1", True, 1.0, 1]
        self.assertEqual("1,True,1.0,1", recursively_join(ar, ","))

    def test_two_layers(self):
        ar = [["1", "2"], "3"]
        self.assertEqual("1,2,3", recursively_join(ar, ","))

    def test_three_layers_mixed(self):
        ar = [["1", "2", ["3", 4], 5, [[[[[6.0]]], 7]]], "8"]
        self.assertEqual("1,2,3,4,5,6.0,7,8", recursively_join(ar, ","))


class TestPrepareForSerialization(TestCase):
    def test_simple_int(self):
        val = prep_object_for_db(1, encode=False)
        self.assertEqual(1, val)
        self.assertIsInstance(val, int)

    def test_simple_bool(self):
        val = prep_object_for_db(False, encode=False)
        self.assertFalse(val)

    def test_simple_float(self):
        val = prep_object_for_db(1.0, encode=False)
        self.assertEqual(1.0, val)
        self.assertIsInstance(val, float)

    def test_simple_string(self):
        val = prep_object_for_db("1.0", encode=False)
        self.assertEqual("1.0", val)
        self.assertIsInstance(val, str)

    def test_list_ints(self):
        val = prep_object_for_db([1, 2, 3], encode=False)
        self.assertListEqual([1, 2, 3], val)
        self.assertIsInstance(val, list)
        self.assertIsInstance(val[0], int)

    def test_dict_mixed(self):
        val = prep_object_for_db({"string": "hi", "int": 1, "bool": True}, encode=False)
        self.assertDictEqual({"string": "hi", "int": 1, "bool": True}, val)

    def test_encoded_int(self):
        val = prep_object_for_db(1, encode=True)
        self.assertEqual("1", val)
        self.assertIsInstance(val, str)

    def test_encoded_bool(self):
        val = prep_object_for_db(False, encode=True)
        self.assertEqual("false", val)
        self.assertIsInstance(val, str)

    def test_encoded_float(self):
        val = prep_object_for_db(1.0, encode=True)
        self.assertEqual("1.0", val)
        self.assertIsInstance(val, str)

    def test_encoded_string(self):
        val = prep_object_for_db("1.0", encode=True)
        self.assertEqual('"1.0"', val)
        self.assertIsInstance(val, str)

    def test_encoded_list_ints(self):
        val = prep_object_for_db([1, 2, 3], encode=True)
        self.assertEqual("[1, 2, 3]", val)
        self.assertIsInstance(val, str)

    def test_encoded_list_mixed(self):
        val = prep_object_for_db([1, "hi", 3.0, True], encode=True)
        self.assertEqual('[1, "hi", 3.0, true]', val)
        self.assertIsInstance(val, str)

    def test_encoded_dict_mixed(self):
        val = prep_object_for_db(
            {"string": "hi", "int": 1, "float": 1.0, "bool": True}, encode=True
        )
        self.assertEqual('{"string": "hi", "int": 1, "float": 1.0, "bool": true}', val)
