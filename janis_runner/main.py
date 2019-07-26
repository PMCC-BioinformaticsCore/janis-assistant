"""
    This could be seen as the "interface" from runner. It should essentially be an interface to all things
    this manager can do. Including running a task, checking the status.

    NO implementation should be here. Please reference external method providers.


"""
from inspect import isclass

import janis
from typing import Optional, Dict, Union, Type

from janis_runner.data.models.filescheme import FileScheme, LocalFileScheme, SSHFileScheme
from janis_runner.engines import Engine, get_engine_type, Cromwell
from janis_runner.environments.environment import Environment
from janis_runner.management.configmanager import ConfigManager
from janis_runner.utils import (
    get_janis_workflow_from_searchname,
    try_parse_dict,
    get_file_from_searchname,
)


def fromjanis(
    workflow: Union[str, janis.Tool, Type[janis.Tool]],
    name: str=None,
    env: Union[str, Environment] = None,
    engine: Union[str, Engine] = None,
    filescheme: Union[str, FileScheme] = None,
    validation_reqs=None,
    hints: Optional[Dict[str, str]] = None,
    output_dir: Optional[str] = None,
    dryrun: bool = False,
    inputs: Union[str, dict] = None,
    watch=True,
    **kwargs,
):

    cm = ConfigManager.manager()

    wf = None
    if isinstance(workflow, janis.Tool):
        wf = workflow
    elif isclass(workflow) and issubclass(workflow, janis.Tool):
        wf = workflow()
    else:
        wf = get_janis_workflow_from_searchname(workflow, ".", name=name)

    inputsdict = None
    if inputs:
        inputsfile = get_file_from_searchname(inputs, ".")
        inputsdict = try_parse_dict(inputsfile)

    environment = None

    if env:
        environment = cm.get_environment(env) if isinstance(env, str) else env
    else:
        eng = get_engine_from_eng(engine)
        fs = get_filescheme_from_fs(filescheme, **kwargs)
        environment = Environment(f"custom_{wf.id()}", eng, fs)

    tm = cm.create_task(
        wf=wf,
        environment=environment,
        validation_requirements=validation_reqs,
        outdir=output_dir,
        hints=hints,
        inputs_dict=inputsdict,
        dryrun=dryrun,
        watch=watch,
    )

    return tm.tid


def get_engine_from_eng(eng, **kwargs):
    if isinstance(eng, Engine):
        return eng

    if eng == "cromwell":
        return Cromwell(cromwell_loc=kwargs.get("cromwell_url"))

    return get_engine_type(eng)()


def get_filescheme_from_fs(fs, **kwargs):
    if isinstance(fs, FileScheme):
        return fs

    if fs == "local":
        return LocalFileScheme()
    elif fs == "ssh":
        con = kwargs.get("filescheme_ssh_binding")
        if not con:
            raise Exception("'filescheme_ssh_binding' was not provided with custom environment")
        return SSHFileScheme(con + "_connection", con)

    raise Exception(f"Couldn't initialise filescheme with unrecognised type: '{fs}'")

def validate_and_run_janis(wf, **kwargs):
    pass
