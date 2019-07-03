import os
import tempfile
from path import Path
from janis import Workflow
from typing import List, Tuple
from inspect import isclass, isabstract

from .logger import Logger


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
    import subprocess

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


def get_file_from_searchname(name, cwd):
    Logger.log(f"Searching for a file called '{name}'")
    if os.path.exists(name):
        Logger.log(f"Found file called '{name}'")
        return name

    Logger.log(f"Searching for file '{name}' in the cwd, '{cwd}'")
    with Path(cwd):
        if os.path.exists(name):
            Logger.log(f"Found file in '{cwd}' called '{name}'")
            return name

    Logger.log(
        f"Attempting to get search path $JANIS_SEARCHPATH from environment variables"
    )
    search_path = os.getenv("JANIS_SEARCHPATH")
    if search_path:
        Logger.log(
            f"Got value for env JANIS_SEARCHPATH '{search_path}', searching for file '{name}' here."
        )
        with Path(search_path):
            if os.path.exists(name):
                Logger.log(f"Found file in '{search_path}' called '{name}'")
                return search_path + name
    else:
        Logger.log("Couldn't find JANIS_SEARCHPATH in environment variables, skipping")

    raise Exception(
        f"Couldn't find a file with filename '{name}' in any of the following: "
        f"full path, current working directory ({cwd}) or the search path."
    )


def try_parse_dict(file: str):
    import ruamel.yaml as ryaml
    import json

    with open(file) as openfile:
        try:
            yaml_dict = ryaml.load(openfile, Loader=ryaml.Loader)

            if isinstance(yaml_dict, dict):
                return yaml_dict
        except Exception as e:
            print(e)

        try:
            json_dict = json.load(openfile)
            if isinstance(json_dict, dict):
                return json_dict
        except Exception as e:
            print(e)

    return None


def get_janis_workflow_from_searchname(name, cwd):
    file = get_file_from_searchname(name, cwd)
    return get_workflow_from_file(file)


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
        raise Exception(
            f"Too many workflows detected ({len(ptypes)}) in '{file}': "
            + ",".join(str(x) for x in ptypes)
        )
    return ptypes[0]


def get_workflows_from_module_spec(spec):
    """
    Get all the Janis.Workflow's that are defined in the file (__module__ == 'module.name')
    :return: List of all the subclasses of a workflow
    """
    potentials = []
    for ptype in spec.__dict__.values():
        if isinstance(ptype, Workflow):
            Logger.warn(
                f"Detected instance of 'Workflow' (id: '{ptype.id()}'), only subclasses are supported"
            )
            continue
        if not callable(ptype):
            continue
        if isabstract(ptype):
            continue
        if not isclass(ptype):
            continue
        if ptype == Workflow:
            continue
        if not issubclass(ptype, Workflow):
            continue
        if ptype.__module__ != "module.name":
            continue
        potentials.append(ptype)

    return potentials
