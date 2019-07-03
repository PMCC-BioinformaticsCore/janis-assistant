"""
    This could be seen as the "interface" from Shepherd. It should essentially be an interface to all things
    this manager can do. Including running a task, checking the status.

    NO implementation should be here. Please reference external method providers.


"""
from inspect import isclass

import janis
from typing import Optional, Dict, Union

from shepherd.environments.environment import Environment
from shepherd.management.configmanager import ConfigManager
from shepherd.utils import (
    get_janis_workflow_from_searchname,
    try_parse_dict,
    get_file_from_searchname,
)


def fromjanis(
    workflow,
    env: Union[str, Environment],
    validation_reqs=None,
    hints: Optional[Dict[str, str]] = None,
    output_dir: Optional[str] = None,
    dryrun: bool = False,
    inputs: Union[str, dict] = None,
    watch=True,
):

    cm = ConfigManager.manager()

    wf = None
    if isinstance(workflow, janis.Tool):
        wf = workflow
    elif isclass(workflow) and issubclass(workflow, janis.Tool):
        wf = workflow()
    else:
        wf = get_janis_workflow_from_searchname(workflow, ".")()

    inputsdict = None
    if inputs:
        inputsfile = get_file_from_searchname(inputs, ".")
        inputsdict = try_parse_dict(inputsfile)

    selectedenv = cm.get_environment(env) if isinstance(env, str) else env

    tm = cm.create_task(
        wf=wf,
        environment=selectedenv,
        validation_requirements=validation_reqs,
        outdir=output_dir,
        hints=hints,
        inputs_dict=inputsdict,
        dryrun=dryrun,
        watch=watch,
    )

    return tm.tid


def validate_and_run_janis(wf, **kwargs):
    pass
