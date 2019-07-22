import argparse

from janis_runner.data.models.schema import TaskStatus

from janis_runner.main import fromjanis
from janis_runner.management.configmanager import ConfigManager
from janis_runner.utils.logger import Logger
from janis_runner.validation import ValidationRequirements

environments = ConfigManager.manager().environmentDB.get_env_ids()


def process_args():
    cmds = {
        "version": do_version,
        "janis": do_janis,
        "run": do_run,
        "watch": do_watch,
        "abort": do_abort,
        "metadata": do_metadata,
        "environment": do_environment,
        "query": do_query,
    }

    parser = argparse.ArgumentParser(description="Execute a workflow")
    subparsers = parser.add_subparsers(help="subcommand help", dest="command")
    parser.add_argument("-d", "--debug", action="store_true")

    subparsers.add_parser("version")
    add_watch_args(subparsers.add_parser("watch"))
    add_abort_args(subparsers.add_parser("abort"))
    add_janis_args(subparsers.add_parser("janis"))
    add_metadata_args(subparsers.add_parser("metadata"))
    add_reconnect_args(subparsers.add_parser("reconnect"))
    add_environment_args(subparsers.add_parser("environment"))
    add_query_args(subparsers.add_parser("query"))
    # add_workflow_args(subparsers.add_parser("run-workflow"))

    args = parser.parse_args()
    return cmds[args.command](args)


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


def add_janis_args(parser):
    from janis import HINTS, HintEnum

    parser.add_argument("workflow", help="Run the workflow defined in this file")
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

    parser.add_argument("-e", "--environment", choices=environments, required=True)

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
            print("Adding " + HintType.key())
            parser.add_argument("--hint-" + HintType.key(), choices=HintType.symbols())
        else:
            print("Skipping " + HintType.key())

    return parser


def add_environment_args(parser):
    parser.add_argument("method", choices=["list", "create", "delete"])
    return parser


def add_reconnect_args(parser):
    parser.add_argument("tid", help="task-id to reconnect to")
    return parser


def add_query_args(parser):
    parser.add_argument("--status", help="workflow status", choices=TaskStatus.all())
    parser.add_argument(
        "--environment",
        help="The environment the task is executing in",
        choices=environments,
    )
    return parser


def do_version(_):
    print("v0.0.2")


def do_run(args):
    Logger.info("Run the shepherd-shepherd with the CommandLine arguments")
    print(args)
    raise NotImplementedError("This path hasn't been implemented yet, raise an issue.")


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


def do_janis(args):
    print(args)

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
        validation_reqs=v,
        env=args.environment,
        hints=hints,
        output_dir=args.output_dir,
        dryrun=args.dryrun,
        inputs=args.inputs,
    )


def do_environment(args):
    method = args.method

    if method == "list":
        return print(ConfigManager.manager().environmentDB.get_env_ids())

    raise NotImplementedError(f"No implementation for '{method}' yet")


def do_query(args):
    status = args.status
    environment = args.environment
    ConfigManager.manager().query_tasks(status, environment)


if __name__ == "__main__":
    process_args()
