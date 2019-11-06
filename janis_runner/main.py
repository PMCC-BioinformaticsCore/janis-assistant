"""
    This could be seen as the "interface" from runner. It should essentially be an interface to all things
    this manager can do. Including running a task, checking the status.

    NO implementation should be here. Please reference external method providers.


"""
import sys, os
from inspect import isclass

import janis_core as j
from typing import Optional, Dict, Union, Type, List

from janis_runner.management.workflowmanager import WorkflowManager

from janis_runner.data.models.filescheme import (
    FileScheme,
    LocalFileScheme,
    SSHFileScheme,
)
from janis_runner.engines import Engine, get_engine_type, Cromwell, EngineType
from janis_runner.environments.environment import Environment
from janis_runner.management.configmanager import ConfigManager
from janis_runner.management.configuration import (
    JanisConfiguration,
    EnvVariables,
    stringify_dict_keys_or_return_value,
)
import janis_runner.templates as janistemplates
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

    fileschemewherelocated = FileScheme.get_type_by_prefix(tool.lower())
    if fileschemewherelocated:
        Logger.info(
            f"Detected remote workflow to localise from '{fileschemewherelocated.__name__}'"
        )
        # Get some unique name for the workflow
        fn = hashlib.md5(tool.lower().encode()).hexdigest() + ".py"
        outdir = os.path.join(JanisConfiguration.manager().configdir, "cached")
        os.makedirs(outdir, exist_ok=True)
        dest = os.path.join(outdir, fn)
        Logger.log(f"Localising '{tool}' to '{dest}'")

        fileschemewherelocated("internal").cp_from(
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

    if not toolref:
        raise Exception(f"Couldn't find tool: '{tool}'")

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
    return wfstr


def generate_inputs(
    tool: Union[str, j.CommandTool, j.Workflow],
    name=None,
    force=False,
    additional_inputs=None,
    with_resources=False,
):
    toolref = resolve_tool(tool, name, from_toolshed=True, force=force)
    inputsdict = None
    if additional_inputs:
        inputsfile = get_file_from_searchname(additional_inputs, ".")
        inputsdict = parse_dict(inputsfile)

    if not toolref:
        raise Exception("Couldn't find workflow with name: " + str(tool))

    return toolref.generate_inputs_override(
        additional_inputs=inputsdict, with_resource_overrides=with_resources
    )


def init_template(templatename, stream=None):
    """
    :param templatename:
    :param force:
    :return:
    """
    import ruamel.yaml

    outpath = EnvVariables.config_path.resolve(True)

    outd = JanisConfiguration.default()

    if templatename:
        schema = janistemplates.get_schema_for_template(
            janistemplates.templates[templatename]
        )
        outd[JanisConfiguration.Keys.Engine] = EngineType.cromwell
        outd[JanisConfiguration.Keys.Template] = {
            s.id(): s.default for s in schema if s.default or not s.optional
        }
        outd[JanisConfiguration.Keys.Template][
            JanisConfiguration.JanisConfigurationTemplate.Keys.Id
        ] = templatename

    outd = stringify_dict_keys_or_return_value(outd)

    if os.path.exists(outpath):
        Logger.info(f"Skipping writing init as config exists at: '{outpath}'")
    else:
        os.makedirs(os.path.dirname(outpath))
        with open(outpath, "w+") as configpath:
            ruamel.yaml.dump(outd, configpath, default_flow_style=False)

    if stream:
        ruamel.yaml.dump(outd, sys.stderr, default_flow_style=False)


def fromjanis(
    workflow: Union[str, j.Tool, Type[j.Tool]],
    name: str = None,
    env: Union[str, Environment] = None,
    engine: Union[str, Engine] = None,
    filescheme: Union[str, FileScheme] = LocalFileScheme(),
    validation_reqs=None,
    hints: Optional[Dict[str, str]] = None,
    output_dir: Optional[str] = None,
    dryrun: bool = False,
    inputs: Union[str, dict] = None,
    required_inputs: dict = None,
    watch=True,
    max_cores=None,
    max_memory=None,
    force=False,
    keep_intermediate_files=False,
    recipes=None,
    should_disconnect=True,
    skip_mysql=False,
    **kwargs,
):
    cm = ConfigManager.manager()
    jc = JanisConfiguration.manager()

    wf = resolve_tool(tool=workflow, name=name, from_toolshed=True, force=force)
    if not wf:
        raise Exception("Couldn't find workflow with name: " + str(workflow))

    if isinstance(wf, j.CommandTool):
        wf = wf.wrapped_in_wf()

    # organise inputs
    inputsdict = {}

    if recipes:
        valuesfromrecipe = jc.recipes.get_recipe_for_keys(recipes)
        inputsdict.update(valuesfromrecipe)

    if inputs:
        if not isinstance(inputs, list):
            inputs = [inputs]
        for inp in inputs:
            if isinstance(inp, dict):
                inputsdict.update(inp)
            else:
                inputsfile = get_file_from_searchname(inp, ".")
                if inputsfile is None:
                    raise FileNotFoundError("Couldn't find inputs file: " + str(inp))
                inputsdict.update(parse_dict(inputsfile))

    if required_inputs:
        reqkeys = set(required_inputs.keys())
        inkeys = set(i.id() for i in wf.inputs())
        invalid_keys = reqkeys - inkeys
        if len(invalid_keys) > 0:
            raise Exception(
                f"There were unrecognised keys when creating inputs for {wf.id()}, {', '.join(invalid_keys)}"
            )
        inputsdict.update(required_inputs)

    validate_inputs(wf, inputsdict)

    row = cm.create_task_base(wf, outdir=output_dir)

    engine = engine or jc.engine

    eng = get_engine_from_eng(
        engine,
        wid=row.wid,
        execdir=WorkflowManager.get_path_for_component_and_dir(
            row.outputdir, WorkflowManager.WorkflowManagerPath.execution
        ),
        confdir=WorkflowManager.get_path_for_component_and_dir(
            row.outputdir, WorkflowManager.WorkflowManagerPath.configuration
        ),
        logfile=os.path.join(
            WorkflowManager.get_path_for_component_and_dir(
                row.outputdir, WorkflowManager.WorkflowManagerPath.logs
            ),
            "engine.log",
        ),
        watch=watch,
        **kwargs,
    )
    fs = get_filescheme_from_fs(filescheme, **kwargs)
    environment = Environment(f"custom_{wf.id()}", eng, fs)

    try:

        tm = cm.start_task(
            wid=row.wid,
            wf=wf,
            environment=environment,
            validation_requirements=validation_reqs,
            task_path=row.outputdir,
            hints=hints,
            inputs_dict=inputsdict,
            dryrun=dryrun,
            watch=watch,
            max_cores=max_cores,
            max_memory=max_memory,
            keep_intermediate_files=keep_intermediate_files,
            should_disconnect=should_disconnect,
            skip_mysql=skip_mysql,
        )
        Logger.log("Finished starting task task")
        return tm.wid

    except KeyboardInterrupt:
        pass

    except Exception as e:
        # Have to make sure we stop the engine if something happens when creating the task that causes
        # janis to exit early
        environment.engine.stop_engine()
        raise e


def validate_inputs(wf, additional_inputs):
    errors = {}

    for inpkey, inp in wf.input_nodes.items():
        value = additional_inputs.get(inpkey, inp.value)

        if inp.datatype.validate_value(value, allow_null_if_not_optional=False):
            continue

        errors[inpkey] = (
            inp.datatype.invalid_value_hint(value)
            or f"An internal error occurred when validating {inpkey} from {inp.datatype.id()}"
        )

    if len(errors) == 0:
        return True

    raise ValueError(f"There were errors in {len(errors)} inputs: " + str(errors))


def get_engine_from_eng(eng, wid, logfile, confdir, watch=True, **kwargs):
    if isinstance(eng, Engine):
        return eng.start_engine()

    if eng == "cromwell":
        url = kwargs.get("cromwell_url")
        if url:
            Logger.info("Found cromwell_url: " + url)
        return Cromwell(
            identifier=f"cromwell-{wid}",
            logfile=logfile,
            confdir=confdir,
            host=url,
            cromwelljar=kwargs.get("cromwell_jar"),
            watch=watch,
        )

    return get_engine_type(eng)(logfile=logfile, watch=watch)


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


def resume(wid):
    wm = ConfigManager.manager().from_wid(wid)
    if not wm:
        raise Exception("Couldn't find workflow manager with wid = " + str(wid))
    wm.resume()


def abort_wids(wids: List[str]):
    for wid in wids:
        try:
            row = ConfigManager.manager().taskDB.get_by_wid(wid)
            WorkflowManager.mark_aborted(row.outputdir)
        except Exception as e:
            Logger.critical(f"Couldn't abort '{wid}': " + str(e))


def cleanup():
    raise NotImplementedError("Implementation coming soon")
    # rows = ConfigManager.manager().taskDB.get_all_tasks()
    # try:
