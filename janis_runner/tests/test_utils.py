from unittest import TestCase
from janis_runner.utils import get_extension, second_formatter


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
