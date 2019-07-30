import argparse

from janis_runner.data.models.schema import TaskStatus

from janis_runner.main import fromjanis
from janis_runner.management.configmanager import ConfigManager
from janis_runner.utils.logger import Logger, LogLevel
from janis_runner.validation import ValidationRequirements

# environments = ConfigManager.manager().environmentDB.get_env_ids()


def process_args(sysargs=None):
    cmds = {
        "version": do_version,
        "run": do_run,
        "watch": do_watch,
        "abort": do_abort,
        "metadata": do_metadata,
        "environment": do_environment,
        "query": do_query,
    }

    parser = argparse.ArgumentParser(description="Execute a workflow")
    add_logger_args(parser)
    parser.add_argument("-v", "--version", action="store_true")

    subparsers = parser.add_subparsers(help="subcommand help", dest="command")
    subparsers.add_parser("version")
    add_watch_args(subparsers.add_parser("watch"))
    add_abort_args(subparsers.add_parser("abort"))
    add_run_args(subparsers.add_parser("run"))
    add_metadata_args(subparsers.add_parser("metadata"))
    add_reconnect_args(subparsers.add_parser("reconnect"))
    add_environment_args(subparsers.add_parser("environment"))
    add_query_args(subparsers.add_parser("query"))
    # add_workflow_args(subparsers.add_parser("run-workflow"))

    args = parser.parse_args(sysargs)

    if args.version:
        return do_version(args)

    check_logger_args(args)

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


def add_watch_args(parser):
    parser.add_argument("tid", help="Task id")
    return parser


def add_metadata_args(parser):
    parser.add_argument("tid", help="Task id")
    return parser


def add_abort_args(parser):
    parser.add_argument("tid", help="Task id")
    return parser


# def add_workflow_args(parser):
#     parser.add_argument("workflow")
#     parser.add_argument("-i", "--inputs", help="workflow inputs")
#     parser.add_argument("-p", "--tools", help="required dependencies")
#     parser.add_argument("-e", "--environment", choices=["local", "local-connect", "pmac"], default="local-connect")
#
#     return parser


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
        "--inputs",
        help="File of inputs (matching the workflow) to override, these inputs will "
        "take precedence over inputs declared in the workflow",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        help="The output directory to which tasks are saved in, defaults to $HOME.",
    )

    parser.add_argument(
        "-e",
        "--environment",
        # choices=environments,
        help="Select a preconfigured environment (takes precendence over engine and filescheme). "
             "See the list of environments with `janis environment list`",
    )
    parser.add_argument(
        "--engine", choices=["cromwell", "cwltool"], default="cwltool", help="Choose an engine to start"
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

    parser.add_argument("--validation-reference", help="reference file for validation")
    parser.add_argument("--validation-truth-vcf", help="truthVCF for validation")
    parser.add_argument("--validation-intervals", help="intervals to validate between")
    parser.add_argument(
        "--validation-fields", nargs="+", help="outputs from the workflow to validate"
    )

    parser.add_argument(
        "--dryrun",
        help="convert workflow, and do everything except submit the workflow",
    )
    parser.add_argument(
        "--no-watch", help="Submit the workflow and return the task id", type=bool
    )

    # add hints
    for HintType in HINTS:
        if issubclass(HintType, HintEnum):
            parser.add_argument("--hint-" + HintType.key(), choices=HintType.symbols())

    return parser


def add_environment_args(parser):
    parser.add_argument("method", choices=["list", "create", "delete"], default="list")
    return parser


def add_reconnect_args(parser):
    parser.add_argument("tid", help="task-id to reconnect to")
    return parser


def add_query_args(parser):
    parser.add_argument("--status", help="workflow status", choices=TaskStatus.all())
    parser.add_argument(
        "--environment",
        help="The environment the task is executing in. See the current list of environments with `janis environment list`",
        # choices=environments,
    )
    return parser


def do_version(_):
    from janis_runner.__meta__ import __version__

    print(__version__)


def do_watch(args):
    tid = args.tid
    tm = ConfigManager.manager().from_tid(tid)
    tm.resume_if_possible()


def do_metadata(args):
    tid = args.tid
    Logger.mute()
    if tid == "*":
        tasks = ConfigManager.manager().taskDB.get_all_tasks()
        for t in tasks:
            try:
                print("--- TASKID = " + t.tid + " ---")
                ConfigManager.manager().from_tid(t.tid).log_dbmetadata()
            except Exception as e:
                print("\tThe following error ocurred: " + str(e))
    else:
        tm = ConfigManager.manager().from_tid(tid)
        tm.log_dbmetadata()
    Logger.unmute()


def do_abort(args):
    tid = args.tid
    tm = ConfigManager.manager().from_tid(tid)
    tm.abort()


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

    return fromjanis(
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
        filescheme_ssh_binding=args.filescheme_ssh_binding,
        cromwell_url=args.cromwell_url,
    )


def do_environment(args):
    method = args.method

    if method == "list":
        return print(", ".join(ConfigManager.manager().environmentDB.get_env_ids()))

    raise NotImplementedError(f"No implementation for '{method}' yet")


def do_query(args):
    status = args.status
    environment = args.environment
    ConfigManager.manager().query_tasks(status, environment)


if __name__ == "__main__":
    process_args()
