import sys
import argparse
import json

import ruamel.yaml
import tabulate
from janis_core import InputQualityType

from janis_core.translationdeps.supportedtranslations import SupportedTranslations

from janis_assistant.__meta__ import DOCS_URL
from janis_assistant.templates.templates import get_template_names
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.management.configuration import JanisConfiguration

from janis_assistant.data.enums.taskstatus import TaskStatus

from janis_assistant.main import (
    fromjanis,
    translate,
    generate_inputs,
    cleanup,
    init_template,
    resume,
    abort_wids,
    spider_tool,
    pause,
)
from janis_assistant.management.configmanager import ConfigManager
from janis_assistant.utils import parse_additional_arguments
from janis_core.utils.logger import Logger, LogLevel

from janis_assistant.utils.batchrun import BatchRunRequirements
from janis_assistant.utils.dateutil import DateUtil
from janis_assistant.validation import ValidationRequirements


class DefaultHelpArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)


def process_args(sysargs=None):
    cmds = {
        "version": do_version,
        "docs": do_docs,
        "run": do_run,
        "translate": do_translate,
        "inputs": do_inputs,
        "watch": do_watch,
        "abort": do_abort,
        "metadata": do_metadata,
        "query": do_query,
        "rm": do_rm,
        "cleanup": do_cleanup,
        "init": do_init,
        "resume": do_resume,
        "pause": do_pause,
        "spider": do_spider,
        "rawquery": do_rawquery,
    }

    parser = DefaultHelpArgParser(description="Execute a workflow")

    add_logger_args(parser)
    parser.add_argument("-v", "--version", action="store_true")

    subparsers = parser.add_subparsers(dest="command")

    add_run_args(subparsers.add_parser("run", help="Run a Janis workflow"))
    add_init_args(
        subparsers.add_parser("init", help="Initialise a Janis configuration")
    )
    add_translate_args(
        subparsers.add_parser(
            "translate", help="Translate a janis workflow to CWL or WDL"
        )
    )
    add_inputs_args(
        subparsers.add_parser(
            "inputs", help="Generate an input job file for a janis workflow"
        )
    )

    add_watch_args(
        subparsers.add_parser("watch", help="Watch an existing Janis workflow")
    )
    add_resume_args(
        subparsers.add_parser(
            "resume", help="INTERNAL: used after submission to monitor the engine"
        )
    )
    add_pause_args(
        subparsers.add_parser(
            "pause",
            help="DEV: If something goes wrong, gracefully shut down Janis without ABORTing the workflow",
        )
    )

    add_abort_args(
        subparsers.add_parser("abort", help="Abort a running Janis Workflow")
    )
    add_rm_args(
        subparsers.add_parser("rm", help="Remove the output directory and metadata")
    )

    add_metadata_args(
        subparsers.add_parser(
            "metadata", help="Print all known metadata about a workflow"
        )
    )
    add_spider_args(
        subparsers.add_parser("spider", help="Get information about a tool")
    )
    add_query_args(
        subparsers.add_parser("query", help="Search known workflows by some criteria")
    )

    add_rawquery_args(
        subparsers.add_parser(
            "rawquery", help="Perform a raw SQL query on the sqlite database of a task"
        )
    )

    subparsers.add_parser("version", help="Print the versions of Janis and exit")
    subparsers.add_parser(
        "docs", help="Attempts to open Janis documentation using `webbrowser`"
    )

    # add_cleanup_args(subparsers.add_parser("cleanup"))

    args = parser.parse_args(sysargs)

    if args.version:
        return do_version(args)

    check_logger_args(args)

    if not args.command:
        parser.print_help()
        sys.exit(2)

    # Don't load the config here anymore, do it conditionally on the route
    # JanisConfiguration.initial_configuration(args.config)

    return cmds[args.command](args)


def add_logger_args(parser):
    parser.add_argument(
        "-d", "--debug", help="log debug", dest="debug", action="store_true"
    )
    parser.add_argument(
        "--verbose", "--logVerbose", help="log verbose", action="store_true"
    )
    parser.add_argument(
        "--logDebug", help="log debug", dest="debug", action="store_true"
    )
    parser.add_argument("--logInfo", help="log info", action="store_true")
    parser.add_argument("--logWarn", help="log warning", action="store_true")
    parser.add_argument("--logCritical", help="log critical", action="store_true")
    parser.add_argument("--logNone", help="log nothing", action="store_true")
    parser.add_argument(
        "-L",
        "--logLevel",
        choices=["VERB", "DEBUG", "INFO", "WARN", "CRITICAL", "NONE"],
    )

    return parser


def add_watch_args(parser):
    parser.add_argument("wid", help="Workflow id")
    return parser


def add_spider_args(parser):
    parser.add_argument("tool", help="Tool to find")
    parser.add_argument(
        "--toolbox", action="store_true", help="Only look for tools in the toolbox"
    )
    parser.add_argument(
        "--name",
        help="Optional name of workflow if there are multiple workflows in the file",
    )
    parser.add_argument(
        "--no-cache",
        help="Force re-download of workflow if remote",
        action="store_true",
    )
    parser.add_argument(
        "--trace",
        action="store_true",
        help="Adds statements to help find why a tool isn't appearing in the toolbox",
    )


def add_resume_args(parser):
    parser.add_argument("wid", help="WID to resume")
    return parser


def add_pause_args(parser):
    parser.add_argument("wid", help="WID to pause")


def add_metadata_args(parser):
    parser.add_argument("wid", help="Task id")
    return parser


def add_abort_args(parser):
    parser.add_argument("wid", help="Task id", nargs="+")
    return parser


def add_rm_args(parser):
    parser.add_argument("wid", help="Task id to remove", nargs="+")
    parser.add_argument("--keep", help="Keep output files", action="store_true")
    return parser


def add_cleanup_args(parser):
    # parser.add_help("Remove janis tasks that can no longer be found")
    return parser


def add_translate_args(parser):
    parser.add_argument("workflow", help="Path to workflow")
    parser.add_argument(
        "translation",
        help="language to translate to",
        choices=SupportedTranslations.all(),
    )
    parser.add_argument(
        "--name",
        help="Optional name of workflow if there are multiple workflows in the tool",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        help="output directory to write output to (default=stdout)",
    )
    parser.add_argument(
        "--no-cache",
        help="Force re-download of workflow if remote",
        action="store_true",
    )
    parser.add_argument(
        "--allow-empty-container",
        action="store_true",
        help="Some tools you use may not include a container, this would usually (and intentionally) cause an error. "
        "Including this flag will disable this check, and empty containers can be used.",
    )


def add_inputs_args(parser):
    parser.add_argument("workflow", help="workflow to generate inputs for")
    parser.add_argument("-c", "--config", help="Path to config file")

    parser.add_argument("-o", "--output", help="file to output to, else stdout")
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Include inputs that have default values",
    )

    parser.add_argument(
        "--user",
        action="store_true",
        help="Only include user inputs (ignore inputs with a qualityType of static or configuration). "
        "Note, this is only available for pilelines that have inputs that are annotated with input qualities.",
    )
    parser.add_argument(
        "--static",
        action="store_true",
        help="Include all inputs with a quality type of static or configuration. "
        "Note, this is only available for pilelines that have inputs that are annotated with input qualities.",
    )

    parser.add_argument(
        "-r",
        "--recipes",
        help="If you have values available from these recipes, they will be ignored from the output",
        nargs="+",
    )
    parser.add_argument(
        "--resources",
        action="store_true",
        help="Add resource overrides into inputs file (eg: runtime_cpu / runtime_memory)",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output to JSON instead of yaml"
    )
    parser.add_argument("-i", "--inputs", help="additional inputs to pull values from")
    parser.add_argument(
        "-n",
        "--name",
        help="If you have multiple workflows in your file, help Janis out to select the right workflow to run",
    )
    parser.add_argument(
        "--no-cache",
        help="Force re-download of workflow if remote",
        action="store_true",
    )
    return parser


def add_run_args(parser):
    from janis_core import HINTS, HintEnum

    parser.add_argument(
        "workflow",
        help="Run the workflow defined in this file or available within the toolbox",
    )

    parser.add_argument("-c", "--config", help="Path to config file")

    parser.add_argument(
        "-i",
        "--inputs",
        help="YAML or JSON inputs file to provide values for the workflow (can specify multiple times)",
        action="append",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        help="This directory to copy outputs to. By default intermediate results are "
        "within a janis/execution subfolder (unless overriden by a template)",
    )

    parser.add_argument("-F", "--foreground", action="store_true", default=False)

    parser.add_argument(
        "-B",
        "--background",
        action="store_true",
        default=False,
        help="Run the workflow engine in the background (or submit to a cluster if your template supports it)",
    )

    parser.add_argument(
        "--progress",
        help="Show the progress screen if running in the background",
        action="store_true",
    )

    parser.add_argument(
        "--keep-intermediate-files",
        action="store_true",
        help="Do not remove execution directory on successful complete",
    )

    parser.add_argument(
        "--skip-file-check",
        action="store_true",
        help="Skip checking if files exist before the start of a workflow.",
    )

    # development settings

    parser.add_argument(
        "--allow-empty-container",
        action="store_true",
        help="Some tools you use may not include a container, this would usually (and intentionally) cause an error. "
        "Including this flag will disable this check, and empty containers can be used.",
    )

    parser.add_argument(
        "--development",
        action="store_true",
        help="Apply common settings (--keep-execution-dir + --mysql) to support incremental development of a pipeline",
    )

    # input manipulation

    inpmanip_args = parser.add_argument_group("input manipulation")

    inpmanip_args.add_argument(
        "-r",
        "--recipe",
        help="Use a provided recipe from a provided template",
        action="append",
    )

    inpmanip_args.add_argument(
        "--max-cores",
        type=int,
        help="maximum number of cores to use when generating resource overrides",
    )
    inpmanip_args.add_argument(
        "--max-memory",
        type=int,
        help="maximum GB of memory to use when generating resource overrides",
    )

    # add hints
    hint_args = parser.add_argument_group("hints")
    for HintType in HINTS:
        if issubclass(HintType, HintEnum):
            hint_args.add_argument(
                "--hint-" + HintType.key(), choices=HintType.symbols()
            )

    # workflow collection

    wfcol_group = parser.add_argument_group("workflow collection arguments")

    wfcol_group.add_argument(
        "--toolbox", help="Only look for tools in the toolbox", action="store_true"
    )

    wfcol_group.add_argument(
        "-n",
        "--name",
        help="If you have multiple workflows in your file, you may want to "
        "help Janis out to select the right workflow to run",
    )

    wfcol_group.add_argument(
        "--no-cache",
        help="Force re-download of workflow if remote",
        action="store_true",
    )

    engine_args = parser.add_argument_group("engine arguments")

    engine_args.add_argument(
        "--engine", choices=EngineType.engines(), help="Choose an engine to start"
    )

    engine_args.add_argument("--cromwell-url", help="Location to Cromwell")

    # batchrun

    batchrun_group = parser.add_argument_group(
        "batchrun arguments",
        # help="This batchrun group modifies the input of the input YAML. "
        # "It will now stack input values that appear in the batchrun ",
    )

    batchrun_group.add_argument(
        "--batchrun", action="store_true", help="Enable the batchrun Pipeline Modifier"
    )
    batchrun_group.add_argument("--batchrun-fields", nargs="+")
    batchrun_group.add_argument(
        "--batchrun-groupby",
        help="Which field should we use to group the samples by, this field should be UNIQUE in the run.",
    )

    # filescheme

    fs_args = parser.add_argument_group("filescheme arguments")
    fs_args.add_argument(
        "-f",
        "--filescheme",
        choices=["local", "ssh"],
        default="local",
        help="Choose the filescheme required to retrieve the output files where your engine is located. "
        "By selecting SSH, Janis will SCP the files using the --filescheme-ssh-binding SSH shortcut.",
    )

    fs_args.add_argument(
        "--filescheme-ssh-binding",
        help="Only valid if you've selected the ssh filescheme. "
        "(eg: scp cluster:/path/to/output local/output/dir)",
    )

    validation_args = parser.add_argument_group("validation arguments")

    validation_args.add_argument(
        "--validation-reference", help="reference file for validation"
    )
    validation_args.add_argument(
        "--validation-truth-vcf", help="truthVCF for validation"
    )
    validation_args.add_argument(
        "--validation-intervals", help="intervals to validate between"
    )
    validation_args.add_argument(
        "--validation-fields", nargs="+", help="outputs from the workflow to validate"
    )

    # beta features

    beta_args = parser.add_argument_group("beta features")

    beta_args.add_argument(
        "--mysql",
        action="store_true",
        help="BETA: Run MySQL for persistence with Cromwell",
    )

    beta_args.add_argument(
        "--no-store",
        action="store_true",
        help="Don't store the workflow ID in the primary database. This means you're unable to watch the workflow by "
        "the ID, and must instead specify the path. This may be useful where multiple jobs are launched at once, and "
        "Janis is unable to appropriately manage locks on the central task database.",
    )

    parser.add_argument("extra_inputs", nargs=argparse.REMAINDER, default=[])

    return parser


def add_reconnect_args(parser):
    parser.add_argument("wid", help="task-id to reconnect to")
    return parser


def add_query_args(parser):
    parser.add_argument("--status", help="workflow status", choices=TaskStatus.all())
    parser.add_argument("--name", help="workflow name")

    return parser


def add_rawquery_args(parser):
    parser.add_argument("wid", help="Workflow ID or task directory")
    parser.add_argument("query", help="RAW SQL query")

    return parser


def check_logger_args(args):
    level = LogLevel.INFO
    if args.verbose:
        level = LogLevel.VERBOSE
    if args.debug:
        level = LogLevel.DEBUG
    if args.logInfo:
        level = LogLevel.INFO
    if args.logWarn:
        level = LogLevel.WARNING
    if args.logCritical:
        level = LogLevel.CRITICAL
    if args.logNone:
        level = None
    if args.logLevel:
        level = LogLevel.from_str(args.logLevel)

    Logger.set_console_level(level)


def add_init_args(args):
    args.add_argument("-r", "--recipes", help="Recipes from template", nargs="+")
    args.add_argument("--stdout", action="store_true", help="Write to standard out")
    args.add_argument(
        "-f", "--force", help="Overwrite the template if it exits", action="store_true"
    )
    args.add_argument(
        "-o",
        "--output",
        help="Location to overwrite to, defaults to: ~/.janis/janis.conf",
    )

    args.add_argument("template", choices=get_template_names())

    args.add_argument("init_params", nargs=argparse.REMAINDER, default=[])


def do_init(args):
    stream = sys.stdout if args.stdout else None
    init_template(
        args.template,
        stream=stream,
        unparsed_init_args=args.init_params,
        output_location=args.output,
        force=args.force,
    )


def do_version(_):
    from tabulate import tabulate
    import importlib_metadata

    from janis_assistant.__meta__ import __version__ as jr_version
    from janis_core.__meta__ import __version__ as jc_version
    import janis_core.toolbox.entrypoints as EP

    fields = [["janis-core", jc_version], ["janis-assistant", jr_version]]
    # eps = pkg_resources.iter_entry_points(group=EP.EXTENSIONS)
    eps = importlib_metadata.entry_points().get(EP.EXTENSIONS, [])
    skip_eps = {"assistant"}
    for entrypoint in eps:
        if entrypoint.name in skip_eps:
            continue
        try:
            version = entrypoint.load().__version__
            if version:
                fields.append(["janis-" + entrypoint.name, version])
            skip_eps.add(entrypoint.name)

        except Exception as e:
            Logger.log_ex(e)

    print(tabulate(fields))


def do_docs(args):
    try:
        import webbrowser

        webbrowser.open(DOCS_URL)
    except Exception as e:
        Logger.critical(f"Failed to open {DOCS_URL} ({e})")


def do_watch(args):
    wid = args.wid
    tm = ConfigManager.manager().from_wid(wid, readonly=True)
    tm.watch()


def do_resume(args):
    resume(args.wid)


def do_pause(args):
    pause(args.wid)


def do_metadata(args):
    wid = args.wid
    Logger.mute()
    if wid == "*":
        tasks = ConfigManager.manager().taskDB.get_all_tasks()
        for t in tasks:
            try:
                print("--- TASKID = " + t.wid + " ---")
                ConfigManager.manager().from_wid(t.wid, readonly=True).log_dbtaskinfo()
            except Exception as e:
                print("\tAn error occurred: " + str(e))
    else:
        tm = ConfigManager.manager().from_wid(wid)
        tm.log_dbtaskinfo()
    Logger.unmute()


def do_abort(args):
    wids = args.wid
    abort_wids(wids)


def do_rm(args):
    wids = args.wid
    for wid in wids:
        try:
            ConfigManager.manager().remove_task(wid, keep_output=args.keep)
        except Exception as e:
            Logger.critical(f"Can't remove {wid}: " + str(e))


def do_run(args):
    JanisConfiguration.initial_configuration(args.config)

    validation_reqs, batchrun_reqs = None, None

    if args.validation_fields:
        Logger.info("Will prepare validation")
        validation_reqs = ValidationRequirements(
            truthVCF=args.validation_truth_vcf,
            reference=args.validation_reference,
            fields=args.validation_fields,
            intervals=args.validation_intervals,
        )

    if args.batchrun:
        Logger.info("Will prepare batch run")
        batchrun_reqs = BatchRunRequirements(
            fields=args.batchrun_fields, groupby=args.batchrun_groupby
        )

    hints = {
        k[5:]: v
        for k, v in vars(args).items()
        if k.startswith("hint_") and v is not None
    }

    # the args.extra_inputs parameter are inputs that we MUST match
    # we'll need to parse them manually and then pass them to fromjanis as requiring a match
    required_inputs = parse_additional_arguments(args.extra_inputs)

    inputs = args.inputs or []
    # we'll manually suck "inputs" out of the extra parms, otherwise it's actually really
    # annoying if you forget to put the inputs before the workflow positional argument.
    # TBH, we could automatically do this for all params, but it's a little trickier

    if "inputs" in required_inputs:
        ins = required_inputs.pop("inputs")
        inputs.extend(ins if isinstance(ins, list) else [ins])
    if "i" in required_inputs:
        ins = required_inputs.pop("i")
        inputs.extend(ins if isinstance(ins, list) else [ins])

    mysql = args.mysql
    keep_intermediate_files = args.keep_intermediate_files is True

    if args.development:
        mysql = True
        keep_intermediate_files = True
        JanisConfiguration.manager().cromwell.call_caching_enabled = True

    wid = fromjanis(
        args.workflow,
        name=args.name,
        validation_reqs=validation_reqs,
        batchrun_reqs=batchrun_reqs,
        engine=args.engine,
        filescheme=args.filescheme,
        hints=hints,
        output_dir=args.output_dir,
        inputs=inputs,
        required_inputs=required_inputs,
        filescheme_ssh_binding=args.filescheme_ssh_binding,
        cromwell_url=args.cromwell_url,
        watch=args.progress,
        max_cores=args.max_cores,
        max_mem=args.max_memory,
        force=args.no_cache,
        recipes=args.recipe,
        keep_intermediate_files=keep_intermediate_files,
        run_in_background=(args.background is True),
        run_in_foreground=(args.foreground is True),
        mysql=mysql,
        only_toolbox=args.toolbox,
        no_store=args.no_store,
        allow_empty_container=args.allow_empty_container,
        check_files=not args.skip_file_check,
    )

    Logger.info("Exiting")
    raise SystemExit


def do_spider(args):
    spider_tool(
        tool=args.tool,
        name=args.name,
        force=args.no_cache,
        only_toolbox=args.toolbox,
        trace=args.trace,
    )


def do_inputs(args):

    if args.config or args.recipes:
        JanisConfiguration.initial_configuration(args.config)

    quality_type = None

    if args.user:
        quality_type = [InputQualityType.user]
    elif args.static:
        quality_type = [InputQualityType.static, InputQualityType.configuration]

    outd = generate_inputs(
        args.workflow,
        all=args.all,
        name=args.name,
        force=args.no_cache,
        additional_inputs=args.inputs,
        with_resources=args.resources,
        quality_type=quality_type,
        recipes=args.recipes,
    )

    if args.json:
        outs = json.dumps(outd, sort_keys=True, indent=4, separators=(",", ": "))
    else:
        outs = ruamel.yaml.dump(outd, default_flow_style=False)

    if args.output:
        with open(args.output, "w+") as out:
            out.write(str(outs))
    else:
        print(outs, file=sys.stdout)


def do_environment(args):
    method = args.method

    if method == "list":
        return print(", ".join(ConfigManager.manager().environmentDB.get_env_ids()))

    raise NotImplementedError(f"No implementation for '{method}' yet")


def do_query(args):
    status = args.status
    name = args.name
    tasks = ConfigManager.manager().query_tasks(status=status, name=name)

    prepared = [
        (
            wid,
            t.status,
            t.name,
            t.start,
            (", ".join(t.labels) if t.labels else ""),
            t.outdir,
        )
        for wid, t in tasks.items()
    ]
    prepared.sort(key=lambda p: p[3] if p[3] else DateUtil.max())

    print(
        tabulate.tabulate(
            prepared,
            headers=["TaskID", "status", "name", "start date", "labels", "path"],
        ),
        file=sys.stdout,
    )


def do_rawquery(args):
    wid = args.wid
    wm = ConfigManager.manager().from_wid(wid, readonly=True)
    result = wm.database.cursor.execute(args.query).fetchall()
    return print(tabulate.tabulate(result))


def do_translate(args):
    translate(
        tool=args.workflow,
        translation=args.translation,
        name=args.name,
        output_dir=args.output_dir,
        force=args.no_cache,
        allow_empty_container=args.allow_empty_container,
    )


def do_cleanup(args):
    cleanup()


if __name__ == "__main__":
    process_args()
