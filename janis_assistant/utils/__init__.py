import re
import socket
import os.path
from typing import Set, List, Union

from janis_core import Tool, Workflow, WorkflowBase
from janis_core.utils.logger import Logger
from .pathhelper import (
    get_janis_workflow_from_searchname,
    get_workflow_from_file,
    get_file_from_searchname,
    parse_dict,
    write_files_into_buffered_zip,
)
from .processlogger import ProcessLogger


def dict_to_yaml_string(d: dict):
    import ruamel.yaml
    from io import StringIO

    yaml = ruamel.yaml.YAML()

    io = StringIO()
    yaml.dump(d, io)
    return io.getvalue()


def generate_new_id(forbiddenids: Set[str]):
    import uuid
    import random

    def gen_uuid():
        return random.choice("abcdef") + str(uuid.uuid4())[:5]

    wid = gen_uuid()
    while wid in forbiddenids:
        wid = gen_uuid()

    return wid


def get_extension(fn):
    """
    Returns the file extension (with no dot) if available, else None
    :param fn:
    :return:
    """
    last_path_component = os.path.basename(fn)
    if "." not in last_path_component:
        return None
    idx = last_path_component[::-1].index(".")
    if idx <= 0:
        return None
    return last_path_component[-idx:]


uri_prefix = re.compile("^[A-z0-9]{2,}:\/\/")


def fully_qualify_filename(fn):
    """
    The theory is, if the user types in a relative path (from the cwd), we should fully qualify this path.
    We'd also want to resolve `~` / `.` and other operators too.
    :param fn:
    :return:
    """
    if fn is None:
        return None
    if isinstance(fn, list):
        return [fully_qualify_filename(f) for f in fn]
    if uri_prefix.match(fn):
        return fn
    return os.path.abspath(os.path.expanduser(os.path.expandvars(fn)))


def second_formatter(secs):
    if secs is None:
        return "N/A"

    if not secs:
        return "0s"

    try:
        remainder = int(secs)
    except:
        Logger.critical(f"second_formatter received non-int type: '{secs}'")
        return str(secs)

    intervals = []
    ranges = [60, 3600, 86400]
    extensions = ["s", "m", "h", "d"]

    under_first_interval = False

    for r in ranges[::-1]:
        if under_first_interval or remainder >= r:
            under_first_interval = True
            val = remainder // r
            remainder -= val * r
            intervals.append(val)

    intervals.append(remainder)

    maxintervals = len(intervals) - 1
    outp = str(intervals[0]) + extensions[maxintervals]
    for i in range(1, len(intervals)):
        ivl = intervals[i]
        outp += ":" + str(ivl).zfill(2) + extensions[maxintervals - i]

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
    if value == "None" or value == "null":
        return None
    if isinstance(value, list):
        return [try_parse_primitive_type(val) for val in value]

    if not isinstance(value, str):
        return value

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
    seen_twice = set()
    if not largs:
        return parsed

    curprefix = None
    curvalue = None

    def add_value_to_parsed(key, value):
        if curprefix not in parsed:
            parsed[key] = value
        elif curprefix not in seen_twice:
            parsed[key] = [parsed[key], value]
            seen_twice.add(key)
        else:
            parsed[key].append(value)

    for arg in largs:
        if arg.startswith("-"):  # starting a new prefix

            if curprefix:
                val = try_parse_primitive_type(curvalue) if curvalue else True
                add_value_to_parsed(curprefix, val)
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
        add_value_to_parsed(curprefix, curvalue if curvalue else True)

    return parsed


def recursively_join(iterable, separator: str):
    return separator.join(
        [
            (recursively_join(i, separator) if isinstance(i, list) else str(i))
            for i in iterable
        ]
    )


def validate_inputs(tool: Tool, additional_inputs):
    errors = {}

    input_values_from_workflow = {}
    if isinstance(tool, WorkflowBase):
        input_values_from_workflow = {
            inpkey: inp.value for inpkey, inp in tool.input_nodes.items() if inp.value
        }
    input_values_to_use = {**input_values_from_workflow, **additional_inputs}

    for inp in tool.tool_inputs():
        inpkey = inp.id()
        value = input_values_to_use.get(inpkey)

        if inp.intype.validate_value(value, allow_null_if_not_optional=False):
            continue

        errors[inpkey] = (
            inp.intype.invalid_value_hint(value)
            or f"An internal error occurred when validating {inpkey} from {inp.intype.id()}"
        )

    if len(errors) == 0:
        return True

    raise ValueError(f"There were errors in {len(errors)} inputs: " + str(errors))


def convert_value_or_list_to_string(value):
    if isinstance(value, list):
        return [convert_value_or_list_to_string(v) for v in value]
    return str(value)


def stringify_value_or_array(value):
    if value is None:
        return None

    if isinstance(value, list):
        return [stringify_value_or_array(s) for s in value]
    return str(value)
