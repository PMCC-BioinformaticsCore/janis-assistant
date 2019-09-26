import socket
import os.path
from typing import Set, List, Union

from .logger import Logger
from .pathhelper import (
    get_janis_workflow_from_searchname,
    get_workflow_from_file,
    get_file_from_searchname,
    parse_dict,
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
    last_path_component = os.path.basename(fn)
    if "." not in last_path_component:
        return None
    idx = last_path_component[::-1].index(".")
    if idx <= 0:
        return None
    return last_path_component[-idx:]


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


def find_free_port():
    s = socket.socket()
    s.bind(("", 0))  # Bind to a free port provided by the host.
    return s.getsockname()[1]  # Return the port number assigned.


def convert_prefix_to_argname(prefix: str):
    if not prefix:
        return None
    return prefix.lstrip("-").replace("-", "_")


def try_parse_primitive_type(value: Union[str, list]):
    if not value:
        return value
    if isinstance(value, list):
        return [try_parse_primitive_type(val) for val in value]

    vl = value.lower()
    if vl == "true":
        return True
    if vl == "false":
        return False

    if vl.isdigit():
        return int(vl)
    if vl.startswith("-") and vl[1:].isdigit():
        return -int(vl[1:])
    try:
        return float(vl)
    except:
        return value


def parse_additional_arguments(largs: List[str]):
    parsed = {}
    if not largs:
        return parsed

    curprefix = None
    curvalue = None

    for arg in largs:
        if arg.startswith("-"):
            if curprefix:
                parsed[curprefix] = (
                    try_parse_primitive_type(curvalue) if curvalue else True
                )
                curvalue = None

            curprefix = convert_prefix_to_argname(arg)
        else:
            parg = try_parse_primitive_type(arg)
            if not curvalue:
                curvalue = parg
            elif curvalue:
                curvalue = (
                    (curvalue + [parg])
                    if isinstance(curvalue, list)
                    else [curvalue, parg]
                )

    if curprefix:
        parsed[curprefix] = curvalue if curvalue else True

    return parsed
