"""
    This could be seen as the "interface" from runner. It should essentially be an interface to all things
    this manager can do. Including running a task, checking the status.

    NO implementation should be here. Please reference external method providers.


"""
import sys, os
from inspect import isclass

import janis_core as j
from typing import Optional, Dict, Union, Type

from janis_runner.data.models.filescheme import (
    FileScheme,
    LocalFileScheme,
    SSHFileScheme,
)
from janis_runner.engines import Engine, get_engine_type, Cromwell
from janis_runner.environments.environment import Environment
from janis_runner.management.configmanager import ConfigManager
from janis_runner.management.configuration import JanisConfiguration
from janis_runner.utils import (
    Logger,
    get_janis_workflow_from_searchname,
    parse_dict,
    get_file_from_searchname,
)

import hashlib


def resolve_tool(
    tool: Union[str, j.CommandTool, Type[j.CommandTool], j.Workflow, Type[j.Workflow]],
    name=None,
    from_toolshed=False,
    force=False,
):
    wf = None
    if isinstance(tool, j.Tool):
        return tool
    elif isclass(tool) and (
        issubclass(tool, j.Workflow) or issubclass(tool, j.CommandTool)
    ):
        return tool()

    if not isinstance(tool, str):
        raise TypeError(
            f"Janis is not sure how to resolve a workflow of type: '{type(tool)}'"
        )

    potential_resolve_type = FileScheme.get_type_by_prefix(tool.lower())
    if potential_resolve_type:
        Logger.info(
            f"Detected remote workflow to localise from '{potential_resolve_type.__name__}'"
        )
        fn = hashlib.md5(tool.lower().encode()).hexdigest() + ".py"
        outdir = os.path.join(JanisConfiguration.manager().configdir, "cached")
        os.makedirs(outdir, exist_ok=True)
        dest = os.path.join(outdir, fn)
        Logger.log(f"Localising '{tool}' to '{dest}'")

        potential_resolve_type("internal").cp_from(
            tool.lower(),
            dest,
            lambda progress: print(f"Download progress: {progress}"),
            force=force,
        )
        tool = dest

    wf = get_janis_workflow_from_searchname(
        tool, ".", name=name, include_commandtools=True
    )

    if wf:
        return wf

    if from_toolshed:
        v = None
        if ":" in tool:
            ps = tool.split(":")
            workflow, v = ps[0], ps[1]

        wf = j.JanisShed.get_tool(tool, v)

    return wf


def translate(
    tool: Union[str, j.CommandTool, Type[j.CommandTool], j.Workflow, Type[j.Workflow]],
    translation: str,
    name: str = None,
    hints: Optional[Dict[str, str]] = None,
    output_dir: Optional[str] = None,
    inputs: Union[str, dict] = None,
    **kwargs,
):

    toolref = resolve_tool(tool, name, from_toolshed=True)

    inputsdict = None
    if inputs:
        inputsfile = get_file_from_searchname(inputs, ".")
        inputsdict = parse_dict(inputsfile)

    if isinstance(toolref, j.Workflow):
        wfstr, _, _ = toolref.translate(
            translation,
            to_console=False,
            to_disk=bool(output_dir),
            export_path=output_dir or "./{language}",
            hints=hints,
            additional_inputs=inputsdict,
        )
    elif isinstance(toolref, j.CommandTool):
        wfstr = toolref.translate(
            translation=translation,
            to_console=False,
            to_disk=bool(output_dir),
            export_path=output_dir or "./{language}",
        )
    else:
        raise Exception("Unsupported tool type: " + toolref.__name__)

    print(wfstr, file=sys.stdout)


def generate_inputs(
    tool: Union[str, j.CommandTool, j.Workflow], name=None, force=False
):
    toolref = resolve_tool(tool, name, from_toolshed=True, force=force)

    if not toolref:
        raise Exception("Couldn't find workflow with name: " + str(toolref))

    return toolref.generate_inputs_override()


def fromjanis(
    workflow: Union[str, j.Tool, Type[j.Tool]],
    name: str = None,
    env: Union[str, Environment] = None,
    engine: Union[str, Engine] = None,
    filescheme: Union[str, FileScheme] = None,
    validation_reqs=None,
    hints: Optional[Dict[str, str]] = None,
    output_dir: Optional[str] = None,
    dryrun: bool = False,
    inputs: Union[str, dict] = None,
    watch=True,
    show_metadata=True,
    max_cores=None,
    max_memory=None,
    force=False,
    **kwargs,
):
    cm = ConfigManager.manager()

    wf = resolve_tool(tool=workflow, name=name, from_toolshed=True, force=force)
    if not wf:
        raise Exception("Couldn't find workflow with name: " + str(workflow))

    if isinstance(wf, j.CommandTool):
        wf = wf.wrapped_in_wf()

    inputsdict = None
    if inputs:
        inputsfile = get_file_from_searchname(inputs, ".")
        inputsdict = parse_dict(inputsfile)

    environment = None

    if env:
        environment = cm.get_environment(env) if isinstance(env, str) else env
    else:
        eng = get_engine_from_eng(engine)
        fs = get_filescheme_from_fs(filescheme, **kwargs)
        environment = Environment(f"custom_{wf.id()}", eng, fs)

    try:

        tm = cm.create_task(
            wf=wf,
            environment=environment,
            validation_requirements=validation_reqs,
            outdir=output_dir,
            hints=hints,
            inputs_dict=inputsdict,
            dryrun=dryrun,
            watch=watch,
            show_metadata=show_metadata,
            max_cores=max_cores,
            max_memory=max_memory,
        )

        return tm.tid

    except KeyboardInterrupt:
        pass

    except Exception as e:
        # Have to make sure we stop the engine if something happens when creating the task that causes
        # janis to exit early
        environment.engine.stop_engine()
        raise e


def get_engine_from_eng(eng, **kwargs):
    if isinstance(eng, Engine):
        return eng.start_engine()

    if eng == "cromwell":
        return Cromwell(
            host=kwargs.get("cromwell_url"), cromwelljar=kwargs.get("cromwell_jar")
        ).start_engine()

    return get_engine_type(eng)().start_engine()


def get_filescheme_from_fs(fs, **kwargs):
    if isinstance(fs, FileScheme):
        return fs

    if fs == "local":
        return LocalFileScheme()
    elif fs == "ssh":
        con = kwargs.get("filescheme_ssh_binding")
        if not con:
            raise Exception(
                "'filescheme_ssh_binding' was not provided with custom environment"
            )
        return SSHFileScheme(con + "_connection", con)

    raise Exception(f"Couldn't initialise filescheme with unrecognised type: '{fs}'")


def validate_and_run_janis(wf, **kwargs):
    pass
