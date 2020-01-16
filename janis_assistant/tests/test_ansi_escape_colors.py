import re
import unittest

from janis_assistant.utils.urwidhelp import translate_color, ANSI_ESCAPE_REGEX


def test_all_colors():
    for i in range(94):
        print(f"\033[0\033[{i}mHello, World!")


class TestAnsiEscapeRegex(unittest.TestCase):
    def test_1(self):
        matches = list(re.finditer(ANSI_ESCAPE_REGEX, "\033[91mHello, world"))
        self.assertEqual(1, len(matches))
        match = matches[0]
        self.assertEqual("91", match.group(1))
        self.assertEqual("Hello, world", match.group(2))

    def test_two_matches(self):
        matches = list(re.finditer(ANSI_ESCAPE_REGEX, "\033[91mHello, world" * 2))
        self.assertEqual(2, len(matches))
        match = matches[1]
        self.assertEqual("91", match.group(1))
        self.assertEqual("Hello, world", match.group(2))

    def test_2_multiline(self):
        match = re.match(ANSI_ESCAPE_REGEX, "\033[91mHello, world\nHi", re.DOTALL)
        self.assertEqual("91", match.group(1))
        self.assertEqual("Hello, world\nHi", match.group(2))


class TestTranslationColors(unittest.TestCase):
    def test_1_of_many(self):
        (fg, bg) = translate_color([91])
        self.assertTrue(fg, "light red")
