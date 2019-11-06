import sys
import argparse
import json

import pkg_resources
import ruamel.yaml
import tabulate

from janis_core.enums.supportedtranslations import SupportedTranslations
from janis_runner.templates.templates import templates as jtemplates
from janis_runner.engines.enginetypes import EngineType
from janis_runner.management.configuration import JanisConfiguration

from janis_runner.data.enums.taskstatus import TaskStatus

from janis_runner.main import (
    fromjanis,
    translate,
    generate_inputs,
    cleanup,
    init_template,
    resume,
    abort_wids,
)
from janis_runner.management.configmanager import ConfigManager
from janis_runner.utils import parse_additional_arguments
from janis_core.utils.logger import Logger, LogLevel

from janis_runner.utils.dateutil import DateUtil
from janis_runner.validation import ValidationRequirements


class DefaultHelpArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)


def process_args(sysargs=None):
    cmds = {
        "version": do_version,
        "run": do_run,
        "translate": do_translate,
        "inputs": do_inputs,
        "watch": do_watch,
        "abort": do_abort,
        "metadata": do_metadata,
        "environment": do_environment,
        "query": do_query,
        "rm": do_rm,
        "cleanup": do_cleanup,
        "init": do_init,
        "resume": do_resume,
    }

    parser = DefaultHelpArgParser(description="Execute a workflow")

    add_logger_args(parser)
    parser.add_argument("-c", "--config", help="Path to config file")
    parser.add_argument("-v", "--version", action="store_true")

    subparsers = parser.add_subparsers(dest="command")

    add_run_args(subparsers.add_parser("run", help="Run a Janis workflow"))
    add_init_args(
        subparsers.add_parser("init", help="Initialise a Janis configuration")
    )
    add_translate_args(
        subparsers.add_parser("translate", help="Translate a janis workflow to ")
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
    # add_environment_args(subparsers.add_parser("environment"))
    add_query_args(
        subparsers.add_parser("query", help="Search known workflows by some criteria")
    )

    subparsers.add_parser("version", help="Print the versions of Janis and exit")

    # add_cleanup_args(subparsers.add_parser("cleanup"))

    args = parser.parse_args(sysargs)

    if args.version:
        return do_version(args)

    check_logger_args(args)

    if not args.command:
        parser.print_help()
        sys.exit(2)

    JanisConfiguration.initial_configuration(args.config)

    return cmds[args.command](args)


def add_logger_args(parser):
    parser.add_argument(
        "-d", "--debug", help="log debug", dest="debug", action="store_true"
    )
    parser.add_argument(
        "--logDebug", help="log debug", dest="debug", action="store_true"
    )
    parser.add_argument("--logInfo", help="log info", action="store_true")
    parser.add_argument("--logWarn", help="log warning", action="store_true")
    parser.add_argument("--logCritical", help="log critical", action="store_true")
    parser.add_argument("--logNone", help="log nothing", action="store_true")
    parser.add_argument(
        "-L", "--logLevel", choices=["DEBUG", "INFO", "WARN", "CRITICAL", "NONE"]
    )

    return parser


def add_watch_args(parser):
    parser.add_argument("wid", help="Workflow id")
    return parser


def add_resume_args(parser):
    parser.add_argument("wid", help="WID to watch")
    return parser


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
        "--output-dir", help="output directory to write output to (default=stdout)"
    )
    parser.add_argument(
        "--no-cache",
        help="Force re-download of workflow if remote",
        action="store_true",
    )


def add_inputs_args(parser):
    parser.add_argument("workflow", help="workflow to generate inputs for")
    parser.add_argument("-o", "--output", help="file to output to, else stdout")
    parser.add_argument("-r", "--recipes", help="Recipes from template", nargs="+")
    parser.add_argument(
        "--resources",
        action="store_true",
        help="Add resource overrides into inputs file",
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

    parser.add_argument("workflow", help="Run the workflow defined in this file")
    parser.add_argument(
        "-n",
        "--name",
        help="If you have multiple workflows in your file, you may want to "
        "help Janis out to select the right workflow to run",
    )

    parser.add_argument(
        "-i",
        "--inputs",
        help="File of inputs (matching the workflow) to override, these inputs will "
        "take precedence over inputs declared in the workflow",
        action="append",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        help="The output directory to which tasks are saved in, defaults to $HOME.",
    )
    parser.add_argument(
        "-r",
        "--recipe",
        help="Use a provided recipe from a provided template",
        action="append",
    )

    parser.add_argument(
        "-e",
        "--environment",
        # choices=environments,
        help="Select a preconfigured environment (takes precendence over engine and filescheme). "
        "See the list of environments with `janis environment list`",
    )
    parser.add_argument(
        "--engine", choices=EngineType.engines(), help="Choose an engine to start"
    )
    parser.add_argument(
        "-f",
        "--filescheme",
        choices=["local", "ssh"],
        default="local",
        help="Choose the filescheme required to retrieve the output files where your engine is located. "
        "By selecting SSH, Janis will SCP the files using the --filescheme-ssh-binding SSH shortcut.",
    )
    parser.add_argument(
        "--filescheme-ssh-binding",
        help="Only valid if you've selected the ssh filescheme. "
        "(eg: scp cluster:/path/to/output local/output/dir)",
    )
    parser.add_argument("--cromwell-url", help="Location to Cromwell")

    parser.add_argument(
        "--keep-intermediate-files",
        action="store_true",
        help="Do not remove execution directory on successful complete",
    )

    parser.add_argument("--validation-reference", help="reference file for validation")
    parser.add_argument("--validation-truth-vcf", help="truthVCF for validation")
    parser.add_argument("--validation-intervals", help="intervals to validate between")
    parser.add_argument(
        "--validation-fields", nargs="+", help="outputs from the workflow to validate"
    )

    parser.add_argument(
        "--dryrun",
        help="convert workflow, and do everything except submit the workflow",
        action="store_true",
    )
    parser.add_argument(
        "--no-watch",
        help="Submit the workflow and return the task id",
        action="store_true",
    )

    parser.add_argument(
        "--no-mysql",
        help="Skip running mysql server for managed Cromwell",
        default=True,
        action="store_true",
    )

    parser.add_argument(
        "--max-cores",
        type=int,
        help="maximum number of cores to use when generating resource overrides",
    )
    parser.add_argument(
        "--max-memory",
        type=int,
        help="maximum GB of memory to use when generating resource overrides",
    )
    parser.add_argument(
        "--no-cache",
        help="Force re-download of workflow if remote",
        action="store_true",
    )

    parser.add_argument(
        "--stay-connected",
        action="store_true",
        default=False,
        help="useful for debugging when something doesn't start correctly",
    )

    # add hints
    for HintType in HINTS:
        if issubclass(HintType, HintEnum):
            parser.add_argument("--hint-" + HintType.key(), choices=HintType.symbols())

    parser.add_argument("extra_inputs", nargs=argparse.REMAINDER, default=[])

    return parser


def add_environment_args(parser):
    parser.add_argument("method", choices=["list", "create", "delete"], default="list")
    return parser


def add_reconnect_args(parser):
    parser.add_argument("wid", help="task-id to reconnect to")
    return parser


def add_query_args(parser):
    parser.add_argument("--status", help="workflow status", choices=TaskStatus.all())
    parser.add_argument("--name", help="workflow name")

    return parser


def check_logger_args(args):
    level = LogLevel.INFO
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
    args.add_argument("template", choices=jtemplates.keys())
    args.add_argument("--stdout", action="store_true", help="Write to standard out")


def do_init(args):
    stream = sys.stdout if args.stdout else None
    init_template(args.template, stream=stream)


def do_version(_):
    from tabulate import tabulate

    from janis_runner.__meta__ import __version__ as jr_version
    from janis_core.__meta__ import __version__ as jc_version
    import janis_core.registry.entrypoints as EP

    fields = [["janis-core", jc_version], ["janis-runner", jr_version]]
    eps = pkg_resources.iter_entry_points(group=EP.EXTENSIONS)
    skip_eps = {"runner"}
    for entrypoint in eps:
        if entrypoint.name in skip_eps:
            continue
        try:
            version = entrypoint.load().__version__
            if version:
                fields.append(["janis-" + entrypoint.name, version])

        except Exception as e:
            Logger.log_ex(e)

    print(tabulate(fields))


def do_watch(args):
    wid = args.wid
    tm = ConfigManager.manager().from_wid(wid)
    tm.watch()


def do_resume(args):
    resume(args.wid)


def do_metadata(args):
    wid = args.wid
    Logger.mute()
    if wid == "*":
        tasks = ConfigManager.manager().taskDB.get_all_tasks()
        for t in tasks:
            try:
                print("--- TASKID = " + t.wid + " ---")
                ConfigManager.manager().from_wid(t.wid).log_dbtaskinfo()
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
    v = None

    if args.validation_fields:
        Logger.info("Will prepare validation")
        v = ValidationRequirements(
            truthVCF=args.validation_truth_vcf,
            reference=args.validation_reference,
            fields=args.validation_fields,
            intervals=args.validation_intervals,
        )

    hints = {
        k[5:]: v
        for k, v in vars(args).items()
        if k.startswith("hint_") and v is not None
    }

    # the args.extra_inputs parameter are inputs that we MUST match
    # we'll need to parse them manually and then pass them to fromjanis as requiring a match
    required_inputs = parse_additional_arguments(args.extra_inputs)

    inputs = args.inputs
    if "inputs" in required_inputs:
        # we'll manually suck "inputs" out of the extra parms, otherwise it's actually really
        # annoying if you forget to put the inputs before the workflow positional argument.
        # TBH, we could automatically do this for all params, but it's a little trickier
        ins = required_inputs.pop("inputs")
        inputs.extend(ins if isinstance(ins, list) else [ins])

    wid = fromjanis(
        args.workflow,
        name=args.name,
        validation_reqs=v,
        env=args.environment,
        engine=args.engine,
        filescheme=args.filescheme,
        hints=hints,
        output_dir=args.output_dir,
        dryrun=args.dryrun,
        inputs=args.inputs,
        required_inputs=required_inputs,
        filescheme_ssh_binding=args.filescheme_ssh_binding,
        cromwell_url=args.cromwell_url,
        watch=not args.no_watch,
        max_cores=args.max_cores,
        max_mem=args.max_memory,
        force=args.no_cache,
        recipes=args.recipe,
        keep_intermediate_files=args.keep_intermediate_files,
        should_disconnect=not (args.stay_connected is True),
        skip_mysql=args.no_mysql,
    )

    Logger.info("Exiting")
    print(wid, file=sys.stdout)
    raise SystemExit


def do_inputs(args):
    outd = generate_inputs(
        args.workflow,
        name=args.name,
        force=args.no_cache,
        additional_inputs=args.inputs,
        with_resources=args.resources,
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


def do_translate(args):
    translate(
        tool=args.workflow,
        translation=args.translation,
        name=args.name,
        output_dir=args.output_dir,
        force=args.no_cache,
    )


def do_cleanup(args):
    cleanup()


if __name__ == "__main__":
    process_args()
