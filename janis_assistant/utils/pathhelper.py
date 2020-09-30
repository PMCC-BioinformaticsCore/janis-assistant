import os
import tempfile
from inspect import isclass, isabstract
from typing import List, Tuple

from janis_core import WorkflowBase, Workflow, CommandTool, Logger, CodeTool
from path import Path


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
    if cwd == ".":
        cwd = os.getcwd()
    Logger.log(f"Searching for a file called '{name}'")
    resolved = os.path.expanduser(name)
    if os.path.exists(resolved) and os.path.isfile(resolved):
        Logger.log(f"Found file called '{name}'")
        return resolved

    Logger.log(f"Searching for file '{name}' in the cwd, '{cwd}'")
    with Path(cwd):
        if os.path.exists(name) and os.path.isfile(resolved):
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
        if os.path.exists(search_path):
            with Path(search_path):
                if os.path.exists(name) and os.path.isfile(resolved):
                    Logger.log(f"Found file in '{search_path}' called '{name}'")
                    return search_path + name
        else:
            Logger.warn(
                f"Search path '{search_path}' (obtained from $JANIS_SEARCHPATH) does not exist "
            )
    else:
        Logger.log("Couldn't find JANIS_SEARCHPATH in environment variables, skipping")

    Logger.log(
        f"Couldn't find a file with filename '{name}' in any of the following: "
        f"full path, current working directory ({cwd}) or the search path."
    )
    return None


def parse_dict(file: str):
    import ruamel.yaml as ryaml
    import json

    err = []

    with open(file) as openfile:
        try:
            yaml_dict = ryaml.load(openfile, Loader=ryaml.Loader)

            if isinstance(yaml_dict, dict):
                return yaml_dict
        except Exception as e:
            err.append(str(e))

        try:
            json_dict = json.load(openfile)
            if isinstance(json_dict, dict):
                return json_dict
        except Exception as e:
            err.append(str(e))

    if err:
        raise Exception(",".join(err))
    raise Exception("Couldn't deserialize " + str(file))


def get_janis_workflow_from_searchname(
    searchpath, cwd, name: str = None, include_commandtools=False
):
    file = get_file_from_searchname(searchpath, cwd)
    if not file:
        return None
    return get_workflow_from_file(file, name, include_commandtools=include_commandtools)


def get_workflow_from_file(file, name, include_commandtools=False):
    # How to import a module given the full path
    # https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
    import importlib.util

    try:
        import sys

        basefilename = os.path.basename(file)

        sys.path.append(os.path.dirname(file))
        spec = importlib.util.spec_from_file_location("module.name", file)
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo)
        ptypes = get_janis_from_module_spec(
            foo, include_commandtools=include_commandtools, name=name
        )

    except Exception as e:
        raise Exception(
            f"Unrecognised python file when getting workflow / command tool: {file} :: {e}"
        )

    # Per https://github.com/PMCC-BioinformaticsCore/janis-core/issues/31, we'll use the following process:
    # 	1. If a `name` is defined:
    # 	    - Force parse every token with a case-insensitive match
    # 	    - If a single item is returned from a case-sensitive match, then use that
    # 	2. If multiple workflows are defined in the same file, use the last defined workflow
    # 	   - This covers the existing _If a single workflow is defined, use that_ case
    # 	3. If no tools were found, raise an Exception
    # 	4. If multiple tools are defined in the file, use the last one:
    # 	   - If a name was defined, `warn` the user that the case-insensitive match returned no results and use the last one
    # 	   - Otherwise, just tell the user we'll use the last defined tool

    ptypes_casesensitive = [(k, v) for (k, v) in ptypes if k == name]

    if len(ptypes_casesensitive) == 1:
        return ptypes_casesensitive[0][1]

    if name is None:
        mains = [v for (k, v) in ptypes if k == "__JANIS_ENTRYPOINT"]
        if len(mains) > 0:
            Logger.debug(
                "Using workflow defined by '__JANIS_ENTRYPOINT' as no name was used"
            )
            return mains[0]

    wftypes = [
        t
        for t in ptypes
        if (
            issubclass(t[1], WorkflowBase)
            if isclass(t[1])
            else isinstance(t[1], WorkflowBase)
        )
    ]
    detected_tokens = ", ".join(f"'{x[0]}' ({x[1].__class__.__name__})" for x in ptypes)

    if len(wftypes) > 0:
        if len(wftypes) > 1:
            if name:
                Logger.warn(
                    f"Providing the `--name` parameter performs a case-insensitive search for the tokens in "
                    f"'{basefilename}, and a case-sensitive search returned no results. You had {len(wftypes)} "
                    f"tokens that matched this search. Janis will use the last one, defined as "
                    f"'{ptypes[-1][0]}' from: {detected_tokens}"
                )
            else:
                Logger.info(
                    f"Multiple workflows were found in '{basefilename}', using '{wftypes[-1][0]}'"
                )
        return wftypes[-1][1]

    if len(ptypes) == 0:
        raise Exception(
            f"There were no valid tools in '{file}', try running with the `--name YourToolName` parameter "
            f"to get more information (it might have abstract / unimplemented methods)."
        )
    if len(ptypes) > 1:

        if name:
            Logger.warn(
                f"Providing the `--name` parameter performs a case-insensitive search for the tokens in "
                f"'{basefilename}, and a case-sensitive search returned no results. You had {len(ptypes)} "
                f"tokens that matched this search. Janis will use the last one, defined as "
                f"'{ptypes[-1][0]}' from: {detected_tokens}"
            )
        else:
            Logger.info(
                f"There were multiple tools (an no workflows) detected in {basefilename}, "
                f"Janis will use '{ptypes[-1][0]}' (the last defined)"
            )

    return ptypes[-1][1]


def get_janis_from_module_spec(spec, include_commandtools=False, name: str = None):
    """
    Get all the Janis.Workflow's that are defined in the file (__module__ == 'module.name')
    :return: List of all the subclasses of a workflow
    """

    if include_commandtools:
        Logger.log("Expanded search to commandtools in " + str(spec))

    potentials = []
    for k, ptype in spec.__dict__.items():
        if name is not None:
            if name.lower() == k.lower():
                potentials.append((k, ptype()))
            continue

        if isinstance(ptype, WorkflowBase) or isinstance(ptype, CommandTool):
            potentials.append((k, ptype))
            continue

        if not callable(ptype):
            continue
        if isabstract(ptype):
            continue
        if not isclass(ptype):
            continue
        if ptype.__module__ != "module.name":
            continue
        if ptype == WorkflowBase or ptype == Workflow:
            continue
        if issubclass(ptype, WorkflowBase):
            potentials.append((k, ptype()))
        if include_commandtools and issubclass(ptype, (CommandTool, CodeTool)):
            potentials.append((k, ptype()))

    return potentials
