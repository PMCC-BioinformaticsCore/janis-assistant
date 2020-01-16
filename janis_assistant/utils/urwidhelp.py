"""
Urwid Helper

    Primarily this script converts an ANSII escaped string for display on urwid.


    Methods:
        - translate_color: Convert a 3/4 bit ANSII escape code into the equivalent urwid color
        - translate_text_for_urwid: Converts an ANSII escaped string into an urwid equivalent.

    Derived from:
        https://github.com/Nanoseb/ncTelegram/blob/master/ncTelegram/ui_msgwidget.py#L218
"""

import re
from typing import Tuple, List, Union

"""
Explained using: https://regex101.com/

    [\x1b\033]          match one of "\x1b" or "\033"
    \[                  match "[" (literal) 
    (                   capture group 1
        [\d;]+              integer or literal ';' literal (one or many)
    )                   end capture group 1
    m                   match "m" literal
    (                   capture group 2
        [^\x1b\033]+        match all characters but "\x1b" or "\033" (text between next group)
    )                   end capture group 2
"""
ANSI_ESCAPE_REGEX = r"[\x1b\033]\[([\d;]+)m([^\x1b\033]+)"

color_list = [
    "black",
    "dark red",
    "dark green",
    "brown",
    "dark blue",
    "dark magenta",
    "dark cyan",
    "light gray",
    "dark gray",
    "light red",
    "light green",
    "yellow",
    "light blue",
    "light magenta",
    "light cyan",
    "white",
]


def translate_color(attr: Union[str, Tuple, List[int]]) -> Tuple[str, str]:
    """
    Translates a 3/4 bit ANSII escape code into the equivalent urwid color:
    Source: https://en.wikipedia.org/wiki/ANSI_escape_code#3/4_bit

    :param attr: string (should be semi-colon (;) delimited) | Tuple | List[int]
    :return: Tuple[foreground: str, background: str]

    """
    if isinstance(attr, int):
        list_attr = [attr]
    elif isinstance(attr, (tuple, list)):
        list_attr = attr
    elif isinstance(attr, str):
        list_attr = [int(i) for i in attr.split(";")]
    else:
        list_attr = [0]

    fg = -1
    bg = -1

    for elem in list_attr:

        if elem == 0:
            fg = 0

        # Foreground (30 - 37)
        if 30 <= elem <= 37:
            fg = elem - 30
        # Background (40 - 47
        elif 40 <= elem <= 47:
            bg = elem - 40
        # Bright foreground
        elif 90 <= elem <= 97:
            fg = elem - 82
        # Bright background
        elif 100 <= elem <= 107:
            bg = elem - 82

    fgcolor = color_list[fg]
    bgcolor = color_list[bg]

    if fg < 0:
        fgcolor = ""
    if bg < 0:
        bgcolor = ""

    return fgcolor, bgcolor


def translate_text_for_urwid(urwid, raw_text):
    """
    Converts an ANSII escaped string into an urwid equivalent.
    First by finding all the matches for "\033[" or "\x1b[",
    reading the ANSII escape code(s) (semi-colon delimited),
    and converting these to the an urwid AttrSpec.

    :param raw_text:
    :return:
    """

    formated_text = []
    if hasattr(raw_text, "decode"):
        raw_text = raw_text.decode("utf-8")

    # Reset the start of text (+ allow for text that isn't formatted)
    if not (raw_text.startswith("\033[") or raw_text.startswith("\x1b[")):
        raw_text = "\x1b[0m" + raw_text

    matches = list(re.finditer(ANSI_ESCAPE_REGEX, raw_text, re.DOTALL))
    for at in matches:
        # crawl to find the first m past the escape sequence
        attr = at.group(1)
        text = at.group(2)

        fgcolor, bgcolor = translate_color(attr)

        formated_text.append((urwid.AttrSpec(fgcolor, bgcolor), text))

    return formated_text
