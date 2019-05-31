import os
import tempfile
from typing import Tuple, List, Set
from inspect import isclass, isabstract

from path import Path   # path.py
from janis import Workflow

from .processlogger import ProcessLogger
from .logger import Logger


def generate_new_id(forbiddenids: Set[str]):
    import uuid

    def gen_uuid(): return str(uuid.uuid4())[:6]

    tid = gen_uuid()
    while tid in forbiddenids:
        tid = gen_uuid()

    return tid


def write_files_into_buffered_zip(files: List[Tuple[str, str]]):
    """
    :param files: [(Filename, Contents)]
    :return: buffered zip
    """
    # I tried to do this all in memory using zipfile.ZipFile,
    # but cromwell never extracts it properly, I'd want it to be:
    # zipfilename.zip -> zipfilename/tools/...listoftools.cwl
    #
    # but it ends up: -> tools/...listoftools.cwl
    import subprocess, os

    base = tempfile.gettempdir() + "/"
    zipfilename = base + "tools.zip"

    if os.path.exists(zipfilename):
        os.remove(zipfilename)
    if os.path.exists(base + "tools"):
        import shutil
        shutil.rmtree(base + "tools")
    os.mkdir(base + "tools")

    for (f, d) in files:
        with open(base + f, "w+") as q:
            q.write(d)
    prevwd = os.getcwd()
    os.chdir(base)
    subprocess.call(["zip", "-r", "tools.zip", "tools/"])
    os.chdir(prevwd)

    return open(zipfilename, "rb")

    # import io, zipfile
    #
    # zip_buf = io.BytesIO()
    #
    # zip = zipfile.ZipFile(zip_buf, mode='w')
    # for (f, d) in files:
    #     zip.writestr(f, d)
    # zip.close()
    # print(zip.printdir())
    # print(zip_buf.getvalue())
    # return zip_buf.getvalue()


def get_extension(fn):
    if "." not in fn: return None
    idx = fn[::-1].index(".")
    if idx <= 0: return None
    return fn[-idx:]


def second_formatter(secs):
    if not secs: return "0"

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


def get_janis_workflow_from_searchname(name, cwd):

    Logger.log(f"Searching for a file called '{name}'")
    if os.path.exists(name):
        Logger.log(f"Found file called '{name}'")
        return get_workflow_from_file(name)

    Logger.log(f"Searching for file '{name}' in the cwd, '{cwd}'")
    with Path(cwd):
        if os.path.exists(name):
            Logger.log(f"Found file in '{cwd}' called '{name}'")
            return get_workflow_from_file(name)

    Logger.log(f"Attempting to get search path $JANIS_SEARCHPATH from environment variables")
    search_path = os.getenv("JANIS_SEARCHPATH")
    if search_path:
        Logger.log(f"Got value for env JANIS_SEARCHPATH '{search_path}', searching for file '{name}' here.")
        with Path(search_path):
            if os.path.exists(name):
                Logger.log(f"Found file in '{search_path}' called '{name}'")
                return get_workflow_from_file(name)
    else:
        Logger.log("Couldn't find JANIS_SEARCHPATH in environment variables, skipping")

    raise Exception("Couldn't find workflow with filename '{name}' in any of the following: "
                    f"full path, current working directory ({cwd}) or the search path.")


def get_workflow_from_file(file):
    # How to import a module given the full path
    # https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
    import importlib.util
    spec = importlib.util.spec_from_file_location("module.name", file)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    ptypes = get_workflows_from_module_spec(foo)
    if len(ptypes) == 0:
        raise Exception(f"Couldn't find any valid workflows in '{file}'.")
    if len(ptypes) > 1:
        raise Exception(f"Too many workflows detected ({len(ptypes)}) in '{file}': " + ','.join(str(x) for x in ptypes))
    return ptypes[0]


def get_workflows_from_module_spec(spec):
    """
    Get all the Janis.Workflow's that are defined in the file (__module__ == 'module.name')
    :return: List of all the subclasses of a workflow
    """
    potentials = []
    for ptype in spec.__dict__.values():
        if isinstance(ptype, Workflow):
            Logger.warn(f"Detected instance of 'Workflow' (id: '{ptype.id()}'), only subclasses are supported")
            continue
        if not callable(ptype): continue
        if isabstract(ptype): continue
        if not isclass(ptype): continue
        if ptype == Workflow: continue
        if not issubclass(ptype, Workflow): continue
        if ptype.__module__ != "module.name": continue
        potentials.append(ptype)

    return potentials
