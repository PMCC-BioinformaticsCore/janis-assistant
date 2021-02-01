"""
    This could be seen as the "interface" from runner. It should essentially be an interface to all things
    this manager can do. Including running a task, checking the status.

    NO implementation should be here. Please reference external method providers.


"""
import os
import sys
import time
from datetime import datetime
from inspect import isclass
from textwrap import dedent
from typing import Optional, Dict, Union, Type, List, Tuple

import janis_core as j
from janis_assistant.data.enums import TaskStatus

from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.modifiers.cwlinputobjectunwrappermodifier import (
    CwlInputObjectUnwrapperModifier,
)

from janis_assistant.modifiers.inputchecker import InputChecker

from janis_assistant.modifiers.inputqualifier import InputFileQualifierModifier

from janis_assistant.modifiers.contigchecker import ContigChecker
from janis_assistant.modifiers.filefinder import FileFinderLocatorModifier
from janis_assistant.modifiers.remotefilemodifier import RemoteFileLocatorModifier
from janis_assistant.modifiers.inputtransformermodifier import InputTransformerModifier
from janis_assistant.validation import ValidationRequirements

from janis_assistant.utils.batchrun import BatchRunRequirements
from janis_core import InputQualityType, Tool, DynamicWorkflow, LogLevel, JanisShed

import janis_assistant.templates as janistemplates
from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.engines import Engine, get_engine_type, Cromwell, EngineType
from janis_assistant.management.configmanager import ConfigManager
from janis_assistant.management.configuration import (
    JanisConfiguration,
    EnvVariables,
    stringify_dict_keys_or_return_value,
    JanisConfigurationEnvironment,
    DatabaseTypeToUse,
    JanisConfigurationCromwell,
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


def run_with_outputs(
    tool: Union[j.CommandTool, j.Workflow],
    inputs: Dict[str, any],
    output_dir: str,
    config: JanisConfiguration = None,
    engine: Optional[str] = None,
    workflow_reference: Optional[str] = None,
):
    """
    Run and WAIT for a Janis workflow to complete. This helper method runs a workflow,
    and returns a dictionary of output values to their output tag. This method MAY throw,
    so ensure it's try-catch wrapped.
    :param tool: An INSTANTIATED tool definition. Seek this from the 'get_janis_workflow_from_searchname' earlier
    :param inputs: A dictionary of pure input values, not file paths.
    :param output_dir: Where to run the execution
    :param config: Optional config, else choose the default at $HOME/.janis/janis.conf
    :param workflow_reference: A reference to the workflow being run, this gets used to write a run.sh file
    :return: A dictionary of output values by the output tag
    """

    job = prepare_job(
        tool=tool,
        output_dir=output_dir,
        required_inputs=inputs,
        jc=config or JanisConfiguration.initial_configuration(None),
        # params to be automatically evaluated
        execution_dir=None,
        inputs={},
        allow_empty_container=False,
        check_files=True,
        container_override={},
        skip_digest_cache=False,
        skip_digest_lookup=False,
        batchrun_reqs=None,
        validation_reqs=None,
        engine=engine,
        hints={},
        keep_intermediate_files=False,
        max_cores=None,
        max_memory=None,
        max_duration=None,
        no_store=True,
        recipes=[],
        run_in_background=None,
        run_in_foreground=None,
        strict_inputs=False,
        watch=False,
        workflow_reference=workflow_reference,
        # don't do extra preprocessing steps
        run_prepare_processing=False,
        localise_all_files=True,
    )

    wm = run_from_jobfile(tool, jobfile=job, wait=True)
    if not wm:
        Logger.critical(f"An error occurred when running workflow {tool.id()}")
        return None

    if not wm.database:
        Logger.critical(
            f"An error occurred when getting the outputs for workflow {tool.id()}"
        )
        return None

    status = wm.database.get_uncached_status()
    if status != TaskStatus.COMPLETED:
        error = ""
        if wm.database.submission_metadata and wm.database.submission_metadata.metadata:
            error = wm.database.submission_metadata.metadata.error or ""
        Logger.critical(
            f"The workflow {tool.id()} ended with status {status}, and hence won't return outputs. Error: {error}"
        )
        return None

    outs = wm.database.outputsDB.get()
    return {o.id_: o.value or o.new_path for o in outs if o.value or o.new_path}


def resolve_tool(
    tool: Union[str, j.CommandTool, Type[j.CommandTool], j.Workflow, Type[j.Workflow]],
    name=None,
    from_toolshed=False,
    force=False,
    only_toolbox=False,
    raises=True,
) -> Tuple[j.Tool, str]:
    if isinstance(tool, j.Tool):
        return tool, tool.id()
    elif isclass(tool) and issubclass(tool, (j.Workflow, j.Tool)):
        t = tool()
        return t, t.id()

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
            outdir = os.path.join(PreparedJob.instance().configdir, "cached")
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

        potential_workflow_ref = get_janis_workflow_from_searchname(
            tool, ".", name=name, include_commandtools=True
        )

        if potential_workflow_ref:
            return potential_workflow_ref

    if from_toolshed:
        toolname, version = tool, None
        if ":" in tool:
            ps = tool.split(":")
            toolname, version = ps[0], ps[1]

        wf = j.JanisShed.get_tool(toolname, version)
        if wf:
            return wf, tool

    if raises:
        raise Exception("Couldn't find tool with name: " + str(tool))


def translate(
    config: JanisConfiguration,
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

    toolref, _ = resolve_tool(tool, name, from_toolshed=True)

    if not toolref:
        raise Exception(f"Couldn't find tool: '{tool}'")

    inputsdict = {}
    if recipes:
        valuesfromrecipe = config.recipes.get_recipe_for_keys(recipes)
        inputsdict.update(valuesfromrecipe)

    inputsdict.update(cascade_inputs(wf=None, inputs=inputs, required_inputs=None,))

    if isinstance(toolref, DynamicWorkflow):
        if not inputsdict:
            raise Exception("Dynamic workflows cannot be translated without the inputs")

        toolref.constructor(inputsdict, hints)
        inputsdict = toolref.modify_inputs(inputsdict, hints)

    container_overrides = container_override
    if not skip_digest_lookup:
        container_overrides = WorkflowManager.prepare_container_override(
            toolref,
            container_override,
            cache_location=config.digest_cache_location,
            skip_digest_cache=skip_digest_cache,
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
    toolref, _ = resolve_tool(
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
    jc: JanisConfiguration,
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
    toolref, _ = resolve_tool(tool, name, from_toolshed=True, force=force)
    inputsdict = None
    if additional_inputs:
        inputsfile = get_file_from_searchname(additional_inputs, ".")
        inputsdict = parse_dict(inputsfile)

    values_to_ignore = set()
    if recipes:
        if not jc:
            Logger.warn(
                "An internal error occurred when getting recipes from your config, please raise an issue on GitHub."
            )
        else:
            for k in jc.recipes.get_recipe_for_keys(recipes):
                values_to_ignore.add(k)

    if not toolref:
        raise Exception("Couldn't find workflow with name: " + str(tool))

    d = toolref.generate_inputs_override(
        additional_inputs=inputsdict,
        with_resource_overrides=with_resources,
        include_defaults=all,
        values_to_ignore=values_to_ignore,
        quality_type=quality_type,
        hints=hints,
    )

    return d


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

                outd["engine"] = EngineType.cromwell
                outd["template"] = {
                    s.id(): parsed.get(s.id(), mapped_schema_to_default.get(s.id()))
                    for s in schema
                    if (s.identifier in parsed)
                    or (
                        s.identifier in mapped_schema_to_default
                        and s.identifier not in keys_to_skip
                    )
                }
                outd["template"]["id"] = templatename

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


def run_from_jobfile(
    workflow: Union[str, j.Tool, Type[j.Tool]],
    jobfile: PreparedJob,
    engine: Union[str, Engine, None] = None,
    wait: bool = False,
    # specific engine args
    cromwell_jar: Optional[str] = None,
    cromwell_url: Optional[str] = None,
):

    cm = ConfigManager(db_path=jobfile.db_path)

    if not workflow:
        raise Exception("Couldn't find workflow with name: " + str(workflow))

    row = cm.create_task_base(wf=workflow, job=jobfile,)

    jobfile.execution_dir = row.execution_dir
    jobfile.output_dir = row.output_dir

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

    eng = get_engine_from_eng(
        engine or jobfile.engine,
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
        cromwell_jar=cromwell_jar,
        cromwell_url=cromwell_url,
    )

    try:

        wm = WorkflowManager.from_janis(
            submission_id=row.submission_id,
            tool=workflow,
            engine=eng,
            prepared_submission=jobfile,
            wait=wait,
        )
        Logger.log("Finished starting task")
        return wm

    except KeyboardInterrupt:
        Logger.info("Exiting...")
        try:
            wm.abort()
        except:
            pass

    except Exception as e:
        # Have to make sure we stop the engine if something happens when creating the task that causes
        # janis to exit early
        eng.stop_engine()
        raise e

    return wm


def prepare_job(
    tool: Union[str, j.Tool, Type[j.Tool]],
    # workflow search options
    workflow_reference: Optional[str],  # if this is None, no jobfile will be written
    jc: JanisConfiguration,
    engine: Optional[str],
    batchrun_reqs: Optional[BatchRunRequirements],
    validation_reqs: Optional[ValidationRequirements],
    hints: Optional[Dict[str, str]],
    output_dir: Optional[str],
    execution_dir: Optional[str],
    inputs: Union[str, dict],
    required_inputs: dict,
    watch,
    max_cores,
    max_memory,
    max_duration,
    keep_intermediate_files,
    recipes,
    run_in_background,
    run_in_foreground,
    no_store,
    allow_empty_container,
    check_files,
    container_override: dict,
    strict_inputs,
    skip_digest_lookup,
    skip_digest_cache,
    run_prepare_processing,
    db_type: DatabaseTypeToUse = None,
    source_hints: List[str] = None,
    post_run_script: str = None,
    localise_all_files: bool = False,
):

    # organise inputs
    inputsdict = {}

    if recipes:
        valuesfromrecipe = jc.recipes.get_recipe_for_keys(recipes)
        inputsdict.update(valuesfromrecipe)

    inputsdict.update(
        cascade_inputs(
            wf=tool,
            inputs=inputs,
            required_inputs=required_inputs,
            batchrun_options=batchrun_reqs,
            strict_inputs=strict_inputs,
        )
    )

    output_dir = generate_output_dir_from(tool.id(), output_dir, jc.output_dir)

    if isinstance(tool, DynamicWorkflow):
        tool.constructor(inputsdict, hints)
        inputsdict = tool.modify_inputs(inputsdict, hints)

    should_run_in_background = (
        run_in_background is True or jc.run_in_background is True
    ) and not (run_in_foreground is True)

    if post_run_script:
        intermediate_prs = get_file_from_searchname(post_run_script, cwd=".")
        if not intermediate_prs or not os.path.exists(intermediate_prs):
            raise Exception(
                f"Couldn't find file for post_run_script '{post_run_script}'"
            )
        post_run_script = intermediate_prs

    # Download remote files to cache directory
    if localise_all_files:
        cache_dir = os.path.join(jc.config_dir, "remote_file_cache")
        m = RemoteFileLocatorModifier(cache_dir=cache_dir)
        inputsdict = m.inputs_modifier(tool, inputsdict, hints)

    if run_prepare_processing:
        cache_dir = os.path.join(output_dir, "janis/prepare")

        Logger.debug(f"Running janis prepare steps at {cache_dir}")

        os.makedirs(cache_dir, exist_ok=True)
        processors = [
            CwlInputObjectUnwrapperModifier(),
            FileFinderLocatorModifier(cache_dir=cache_dir, source_hints=source_hints),
            InputFileQualifierModifier(),
            InputTransformerModifier(cache_dir=cache_dir),
            InputChecker(check_file_existence=True),
            ContigChecker(),
        ]

        tool_to_evaluate, new_inputs = PipelineModifierBase.apply_many(
            processors, tool, inputsdict, hints=hints
        )
        inputsdict = new_inputs

    submission = PreparedJob(
        # job stuff
        workflow_reference=workflow_reference,
        config_dir=jc.config_dir,
        db_path=jc.db_path,
        execution_dir=execution_dir,
        engine=engine or jc.engine,
        cromwell=jc.cromwell,
        template=jc.template,
        notifications=jc.notifications,
        environment=JanisConfigurationEnvironment(
            max_cores=max_cores or jc.environment.max_cores,
            max_memory=max_memory or jc.environment.max_memory,
            max_duration=max_duration or jc.environment.max_duration,
        ),
        run_in_background=should_run_in_background,
        digest_cache_location=jc.digest_cache_location,
        # job information
        inputs=inputsdict,
        output_dir=output_dir,
        keep_intermediate_files=keep_intermediate_files,
        recipes=recipes,
        hints=hints,
        allow_empty_container=allow_empty_container,
        container_override=container_override,
        skip_digest_lookup=skip_digest_lookup,
        skip_digest_cache=skip_digest_cache,
        batchrun=batchrun_reqs,
        store_in_central_db=not no_store,
        skip_file_check=not check_files,
        strict_inputs=strict_inputs,
        validation=validation_reqs,
        # config stuff
        should_watch_if_background=watch,
        call_caching_enabled=jc.call_caching_enabled,
        container_type=jc.container.get_container_type(),
        post_run_script=post_run_script,
    )

    if db_type:
        submission.cromwell = submission.cromwell or JanisConfigurationCromwell()
        submission.cromwell.db_type = db_type

    return submission


# output dir
def generate_output_dir_from(wf_id, output_dir, jc_output_dir):
    if not output_dir and not jc_output_dir:
        raise Exception(
            f"You must specify an output directory (or specify an 'output_dir' "
            f"in your configuration)"
        )

    default_outdir = None
    if jc_output_dir:
        default_outdir = os.path.join(jc_output_dir, wf_id)

    if not output_dir:
        od = default_outdir
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(od, dt)

    output_dir = fully_qualify_filename(output_dir)

    return output_dir


def get_engine_from_eng(
    eng,
    wid,
    logfile,
    confdir,
    execdir: str,
    cromwell_url: Optional[str],
    cromwell_jar: Optional[str],
):

    engid = str(eng)
    if engid == EngineType.cromwell.value:
        url = cromwell_url or PreparedJob.instance().cromwell.url
        if url:
            Logger.info("Found cromwell_url: " + url)
        return Cromwell(
            identifier=f"cromwell-{wid}",
            logfile=logfile,
            confdir=confdir,
            host=url,
            cromwelljar=cromwell_jar,
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


def abort_wids(sids: List[str], wait=True):
    cm = ConfigManager(db_path=None)
    for sid in sids:
        try:
            row = cm.get_row_for_submission_id_or_path(sid)
            WorkflowManager.mark_aborted(row.execution_dir, row.submission_id)
        except Exception as e:
            Logger.critical(f"Couldn't abort '{sid}': " + str(e))
            raise e

    if wait:
        Logger.info(
            "Waiting until completely aborted. This can take up to a few minutes to complete."
        )
        for sid in sids:
            try:
                wm = ConfigManager.get_from_path_or_submission_lazy(sid, readonly=True)
                check_attempts = 0
                while not wm.database.get_uncached_status().is_in_final_state():
                    time.sleep(1)
                    check_attempts += 1
                    if check_attempts % 5 == 0:
                        Logger.info(f"Still waiting for '{sid}' to move to final state")
            except Exception as e:
                Logger.critical(f"Couldn't watch '{sid}' until aborted: {str(e)}")
        Logger.info(f"Jobs {' '.join(sids)} should be completely aborted now")


def cleanup():
    ConfigManager(db_path=None).cleanup_missing_tasks()
