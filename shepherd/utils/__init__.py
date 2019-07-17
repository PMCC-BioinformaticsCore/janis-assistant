from typing import Set

from .logger import Logger
from .pathhelper import (
    get_janis_workflow_from_searchname,
    get_workflow_from_file,
    get_file_from_searchname,
    try_parse_dict,
    write_files_into_buffered_zip,
)
from .processlogger import ProcessLogger


def generate_new_id(forbiddenids: Set[str]):
    import uuid
    import random

    def gen_uuid():
        return random.choice("abcdef") + str(uuid.uuid4())[:5]

    tid = gen_uuid()
    while tid in forbiddenids:
        tid = gen_uuid()

    return tid


def get_extension(fn):
    if "." not in fn:
        return None
    idx = fn[::-1].index(".")
    if idx <= 0:
        return None
    return fn[-idx:]


def second_formatter(secs):
    if not secs:
        return "0"

    intervals = []
    ranges = [60, 3600, 86400]

    remainder = secs
    under_first_interval = False

    for r in ranges[::-1]:
        if under_first_interval or remainder >= r:
            under_first_interval = True
            val = remainder // r
            remainder -= val * r
            intervals.append(val)

    intervals.append(remainder)

    outp = str(intervals[0])
    for ivl in intervals[1:]:
        outp += ":" + str(ivl).zfill(2)

    return outp
