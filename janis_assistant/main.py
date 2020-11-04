"""
    This could be seen as the "interface" from runner. It should essentially be an interface to all things
    this manager can do. Including running a task, checking the status.

    NO implementation should be here. Please reference external method providers.


"""
import os
import sys
from inspect import isclass
from textwrap import dedent
from typing import Optional, Dict, Union, Type, List

import janis_core as j
from janis_core import InputQualityType, Tool, DynamicWorkflow, LogLevel, JanisShed

import janis_assistant.templates as janistemplates
from janis_assistant.engines import Engine, get_engine_type, Cromwell, EngineType
from janis_assistant.management.configmanager import ConfigManager
from janis_assistant.management.configuration import (
    JanisConfiguration,
    EnvVariables,
    stringify_dict_keys_or_return_value,
)
from janis_assistant.management.filescheme import (
    FileScheme,
    LocalFileScheme,
    SSHFileScheme,
)
from janis_assistant.management.workflowmanager import WorkflowManager
from janis_assistant.templates import TemplateInput
from janis_assistant.utils import (
    Logger,
    get_janis_workflow_from_searchname,
    parse_dict,
    get_file_from_searchname,
    fully_qualify_filename,
)
from janis_assistant.utils.inputshelper import cascade_inputs


def resolve_tool(
    tool: Union[str, j.CommandTool, Type[j.CommandTool], j.Workflow, Type[j.Workflow]],
    name=None,
    from_toolshed=False,
    force=False,
    only_toolbox=False,
):
    if isinstance(tool, j.Tool):
        return tool
    elif isclass(tool) and issubclass(tool, (j.Workflow, j.Tool)):
        return tool()

    if not isinstance(tool, str):
        raise TypeError(
            f"Janis is not sure how to resolve a workflow of type: '{type(tool)}'"
        )

    if not only_toolbox:
        fileschemewherelocated = FileScheme.get_type_by_prefix(tool.lower())
        if fileschemewherelocated != LocalFileScheme and fileschemewherelocated:
            Logger.info(
                f"Detected remote workflow to localise from '{fileschemewherelocated.__name__}'"
            )
            # Get some unique name for the workflow
            import hashlib

            fn = hashlib.md5(tool.lower().encode()).hexdigest() + ".py"
            outdir = os.path.join(JanisConfiguration.manager().configdir, "cached")
            os.makedirs(outdir, exist_ok=True)
            dest = os.path.join(outdir, fn)
            Logger.log(f"Localising '{tool}' to '{dest}'")

            fileschemewherelocated("internal").cp_from(
                source=tool.lower(),
                dest=dest,
                report_progress=lambda progress: print(
                    f"Download progress: {progress}"
                ),
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
            tool, v = ps[0], ps[1]

        wf = j.JanisShed.get_tool(tool, v)

        return wf


def translate(
    tool: Union[str, j.CommandTool, Type[j.CommandTool], j.Workflow, Type[j.Workflow]],
    translation: str,
    name: str = None,
    hints: Optional[Dict[str, str]] = None,
    output_dir: Optional[str] = None,
    inputs: Union[str, dict] = None,
    allow_empty_container=False,
    container_override=None,
    skip_digest_lookup=False,
    skip_digest_cache=False,
    recipes: List[str] = None,
    **kwargs,
):

    toolref = resolve_tool(tool, name, from_toolshed=True)

    if not toolref:
        raise Exception(f"Couldn't find tool: '{tool}'")

    inputsdict = {}
    if recipes:
        jc = JanisConfiguration.manager()
        valuesfromrecipe = jc.recipes.get_recipe_for_keys(recipes)
        inputsdict.update(valuesfromrecipe)

    inputsdict.update(
        cascade_inputs(
            wf=None,
            inputs=inputs,
            required_inputs=None,
        )
    )

    if isinstance(toolref, DynamicWorkflow):
        if not inputsdict:
            raise Exception("Dynamic workflows cannot be translated without the inputs")

        toolref.constructor(inputsdict, hints)
        inputsdict = toolref.modify_inputs(inputsdict, hints)

    container_overrides = container_override
    if not skip_digest_lookup:
        container_overrides = WorkflowManager.prepare_container_override(
            toolref, container_override, skip_digest_cache=skip_digest_cache
        )

    if isinstance(toolref, j.WorkflowBase):
        wfstr, _, _ = toolref.translate(
            translation,
            to_console=False,
            to_disk=bool(output_dir),
            export_path=output_dir or "./{language}",
            hints=hints,
            additional_inputs=inputsdict,
            allow_empty_container=allow_empty_container,
            container_override=container_overrides,
        )
    elif isinstance(toolref, (j.CommandTool, j.CodeTool)):
        wfstr = toolref.translate(
            translation=translation,
            to_console=False,
            to_disk=bool(output_dir),
            export_path=output_dir or "./{language}",
            allow_empty_container=allow_empty_container,
            container_override=container_overrides,
        )

    else:
        name = toolref.__name__ if isclass(toolref) else toolref.__class__.__name__
        raise Exception("Unsupported tool type: " + name)

    print(wfstr, file=sys.stdout)
    return wfstr


def spider_tool(
    tool: Union[str, j.CommandTool, j.Workflow],
    name=None,
    force=False,
    only_toolbox=False,
    trace=False,
    print_all_tools=False,
):
    j.JanisShed.should_trace = trace
    toolref = resolve_tool(
        tool, name, from_toolshed=True, force=force, only_toolbox=only_toolbox
    )

    if print_all_tools:
        from tabulate import tabulate

        tools = [
            [tools[0].id(), ", ".join(t.version() for t in tools)]
            for tools in JanisShed.get_all_tools()
        ]

        print(tabulate(sorted(tools, key=lambda k: k[0].casefold())))

    if not toolref:
        raise Exception(f"Couldn't find tool: '{tool}'")

    print(toolref.help())


def generate_inputs(
    tool: Union[str, j.CommandTool, j.Workflow],
    all=False,
    name=None,
    force=False,
    additional_inputs=None,
    with_resources=False,
    quality_type: List[InputQualityType] = None,
    recipes: List[str] = None,
    hints: dict = None,
):
    toolref = resolve_tool(tool, name, from_toolshed=True, force=force)
    inputsdict = None
    if additional_inputs:
        inputsfile = get_file_from_searchname(additional_inputs, ".")
        inputsdict = parse_dict(inputsfile)

    values_to_ignore = set()
    if recipes:
        jc = JanisConfiguration.manager()
        for k in jc.recipes.get_recipe_for_keys(recipes):
            values_to_ignore.add(k)

    if not toolref:
        raise Exception("Couldn't find workflow with name: " + str(tool))

    return toolref.generate_inputs_override(
        additional_inputs=inputsdict,
        with_resource_overrides=with_resources,
        include_defaults=all,
        values_to_ignore=values_to_ignore,
        quality_type=quality_type,
        hints=hints,
    )


import argparse


class InitArgParser(argparse.ArgumentParser):
    def __init__(
        self, templatename, schema: List[TemplateInput], description: str = None
    ):
        super().__init__(f"janis init {templatename}", description=description)
        # self.add_usage(
        #     , self._actions, self._mutually_exclusive_groups
        # )

        self.templatename = templatename
        self.required_args = set()

        required_parser = self.add_argument_group("required arguments")
        optional_parser = self.add_argument_group("optional arguments")

        for s in schema:
            action = None

            default = None
            if s.type == bool:
                action = "store_false" if s.default else "store_true"
                default = argparse.SUPPRESS

            group = optional_parser
            if not s.optional:
                group = required_parser
                self.required_args.add(s.identifier)

            hlp = s.doc
            if s.default is not None:
                hlp = f"(default: {s.default}) {hlp}"

            group.add_argument(
                "--" + s.identifier,
                action=action,
                required=not s.optional,
                help=hlp,
                default=default,
            )

    def parse_args(self, args=None, namespace=None):
        parsed = vars(super().parse_args(args, namespace))

        # confirm parsed args
        filtered_args = {p: v for p, v in parsed.items() if parsed[p] is not None}
        missing_args = self.required_args - set(filtered_args.keys())
        if len(missing_args) > 0:
            self.error(f"expected arguments: {', '.join(missing_args)}")

        return filtered_args

    def error(self, message):
        sys.stderr.write(
            f"There was an error initializing '{self.templatename}': {message}\n"
        )
        self.print_help()
        sys.exit(2)


def init_template(
    templatename,
    stream=None,
    unparsed_init_args=None,
    output_location=None,
    force=False,
):
    """
    :param templatename:
    :param force:
    :return:
    """
    import ruamel.yaml

    outpath = fully_qualify_filename(
        output_location or EnvVariables.config_path.resolve(True)
    )

    cached_outd = None

    def get_config():
        """
        This is here to lazily instantiate the config
        """
        nonlocal cached_outd
        if not cached_outd:

            outd = JanisConfiguration.default()

            if templatename:
                tmpl = janistemplates.get_template(templatename)
                schema = janistemplates.get_schema_for_template(tmpl)

                mapped_schema_to_default = {
                    s.identifier: s.default for s in schema if s.default is not None
                }

                # parse extra params
                description = dedent(tmpl.__doc__) if tmpl.__doc__ else None

                parser = InitArgParser(templatename, schema, description=description)
                parsed = parser.parse_args(unparsed_init_args)

                try:
                    # "easier to ask for forgiveness than permission" https://stackoverflow.com/a/610923
                    keys_to_skip = set(tmpl.ignore_init_keys)
                except AttributeError:
                    Logger.log(
                        f"Template '{templatename}' didn't have 'ignore_init_keys'"
                    )
                    keys_to_skip = set()

                outd[JanisConfiguration.Keys.Engine] = EngineType.cromwell
                outd[JanisConfiguration.Keys.Template] = {
                    s.id(): parsed.get(s.id(), mapped_schema_to_default.get(s.id()))
                    for s in schema
                    if (s.identifier in parsed)
                    or (
                        s.identifier in mapped_schema_to_default
                        and s.identifier not in keys_to_skip
                    )
                }
                outd[JanisConfiguration.Keys.Template][
                    JanisConfiguration.JanisConfigurationTemplate.Keys.Id
                ] = templatename

            cached_outd = stringify_dict_keys_or_return_value(outd)
        return cached_outd

    if any(k in unparsed_init_args for k in ("-h", "--help")):
        get_config()

    does_exist = os.path.exists(outpath)
    if does_exist and not force:
        Logger.info(f"Janis will skip writing config as file exists at: '{outpath}'")
    else:
        if does_exist:
            Logger.info(f"Overwriting template at '{outpath}'")
        else:
            Logger.info(f"Saving Janis config to '{outpath}'")

        os.makedirs(os.path.dirname(outpath), exist_ok=True)
        val = get_config()
        with open(outpath, "w+") as configpath:
            ruamel.yaml.dump(val, configpath, default_flow_style=False)

    if stream:
        ruamel.yaml.dump(get_config(), sys.stdout, default_flow_style=False)


def fromjanis(
    workflow: Union[str, j.Tool, Type[j.Tool]],
    name: str = None,
    engine: Union[str, Engine] = None,
    validation_reqs=None,
    batchrun_reqs=None,
    hints: Optional[Dict[str, str]] = None,
    output_dir: Optional[str] = None,
    execution_dir: Optional[str] = None,
    dryrun: bool = False,
    inputs: Union[str, dict] = None,
    required_inputs: dict = None,
    watch=True,
    max_cores=None,
    max_memory=None,
    max_duration=None,
    force=False,
    keep_intermediate_files=False,
    recipes=None,
    run_in_background=True,
    run_in_foreground=None,
    dbconfig=None,
    only_toolbox=False,
    no_store=False,
    allow_empty_container=False,
    check_files=True,
    container_override: dict = None,
    strict_inputs=False,
    **kwargs,
):
    cm = ConfigManager.manager()
    jc = JanisConfiguration.manager()

    wf: Optional[Tool] = resolve_tool(
        tool=workflow,
        name=name,
        from_toolshed=True,
        only_toolbox=only_toolbox,
        force=force,
    )
    if not wf:
        raise Exception("Couldn't find workflow with name: " + str(workflow))

    # if isinstance(tool, j.CommandTool):
    #     tool = tool.wrapped_in_wf()
    # elif isinstance(tool, j.CodeTool):
    #     tool = tool.wrapped_in_wf()

    # organise inputs
    inputsdict = {}

    if recipes:
        valuesfromrecipe = jc.recipes.get_recipe_for_keys(recipes)
        inputsdict.update(valuesfromrecipe)

    inputsdict.update(
        cascade_inputs(
            wf=wf,
            inputs=inputs,
            required_inputs=required_inputs,
            batchrun_options=batchrun_reqs,
            strict_inputs=strict_inputs,
        )
    )

    if isinstance(wf, DynamicWorkflow):
        wf.constructor(inputsdict, hints)
        inputsdict = wf.modify_inputs(inputsdict, hints)

    row = cm.create_task_base(
        wf,
        outdir=output_dir,
        execution_dir=execution_dir,
        store_in_centraldb=not no_store,
    )

    # set logger for submit
    Logger.set_write_level(Logger.CONSOLE_LEVEL)
    logpath = os.path.join(
        WorkflowManager.get_path_for_component_and_dir(
            row.execution_dir, WorkflowManager.WorkflowManagerPath.logs
        ),
        "janis-submit.log",
    )
    Logger.WRITE_LEVELS = {Logger.CONSOLE_LEVEL: (logpath, open(logpath, "a"))}
    Logger.debug(f"Set submission logging to '{logpath}'")
    print(row.submission_id, file=sys.stdout)

    engine = engine or jc.engine

    eng = get_engine_from_eng(
        engine,
        wid=row.submission_id,
        execdir=WorkflowManager.get_path_for_component_and_dir(
            row.execution_dir, WorkflowManager.WorkflowManagerPath.execution
        ),
        confdir=WorkflowManager.get_path_for_component_and_dir(
            row.execution_dir, WorkflowManager.WorkflowManagerPath.configuration
        ),
        logfile=os.path.join(
            WorkflowManager.get_path_for_component_and_dir(
                row.execution_dir, WorkflowManager.WorkflowManagerPath.logs
            ),
            "engine.log",
        ),
        watch=watch,
        **kwargs,
    )

    try:

        # Note: run_in_foreground can be None, so
        # (not (run_in_foreground is True)) != (run_in_foreground is False)

        should_run_in_background = (
            run_in_background is True or jc.run_in_background is True
        ) and not (run_in_foreground is True)

        tm = cm.start_task(
            submission_id=row.submission_id,
            tool=wf,
            engine=eng,
            validation_requirements=validation_reqs,
            batchrun_requirements=batchrun_reqs,
            output_dir=row.output_dir,
            execution_dir=row.execution_dir,
            hints=hints,
            inputs_dict=inputsdict,
            dryrun=dryrun,
            watch=watch,
            max_cores=max_cores,
            max_memory=max_memory,
            max_duration=max_duration,
            keep_intermediate_files=keep_intermediate_files,
            run_in_background=should_run_in_background,
            dbconfig=dbconfig,
            allow_empty_container=allow_empty_container,
            container_override=container_override,
            check_files=check_files,
            **kwargs,
        )
        Logger.log("Finished starting task")
        return tm

    except KeyboardInterrupt:
        Logger.info("Exiting...")

    except Exception as e:
        # Have to make sure we stop the engine if something happens when creating the task that causes
        # janis to exit early
        eng.stop_engine()
        raise e


def get_engine_from_eng(eng, wid, logfile, confdir, execdir: str, **kwargs):

    if eng == "cromwell":
        url = kwargs.get("cromwell_url") or JanisConfiguration.manager().cromwell.url
        if url:
            Logger.info("Found cromwell_url: " + url)
        return Cromwell(
            identifier=f"cromwell-{wid}",
            logfile=logfile,
            confdir=confdir,
            host=url,
            cromwelljar=kwargs.get("cromwell_jar"),
            execution_dir=execdir,
        )

    return get_engine_type(eng)(logfile=logfile, execution_dir=execdir)


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


def resume(wid, foreground: bool = False):
    wm = ConfigManager.manager().from_submission_id_or_path(wid, readonly=False)
    if not wm:
        raise Exception("Couldn't find workflow manager with wid = " + str(wid))

    run_in_background = False
    if foreground:
        run_in_background = False
    elif wm.database.workflowmetadata.configuration.run_in_background:
        run_in_background = True

    wm.start_or_submit(run_in_background=run_in_background)


def pause(wid):
    wm = ConfigManager.manager().from_submission_id_or_path(wid)
    if not wm:
        raise Exception("Couldn't find workflow manager with wid = " + str(wid))
    wm.mark_paused(wm.execution_dir)


def abort_wids(sids: List[str]):
    for sid in sids:
        try:
            row = ConfigManager.manager().get_row_for_submission_id_or_path(sid)
            WorkflowManager.mark_aborted(row.execution_dir, row.submission_id)
        except Exception as e:
            Logger.critical(f"Couldn't abort '{sid}': " + str(e))
            raise e


def cleanup():
    ConfigManager.manager().cleanup_missing_tasks()
