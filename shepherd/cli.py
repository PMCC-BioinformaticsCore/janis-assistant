import argparse

from janis import HINTS, HintEnum

from shepherd.utils.logger import Logger
from shepherd.management.configmanager import ConfigManager


def process_args():
    cmds = {
        "version": do_version,
        "janis": do_janis,
        "run": do_run,
        "watch": do_watch,
        "abort": do_abort
    }

    parser = argparse.ArgumentParser(description="Execute a workflow")
    subparsers = parser.add_subparsers(help="subcommand help", dest="command")
    parser.add_argument("-d", "--debug", action="store_true")

    subparsers.add_parser("version")
    add_watch_args(subparsers.add_parser("watch"))
    add_abort_args(subparsers.add_parser("abort"))
    add_janis_args(subparsers.add_parser("janis"))
    add_reconnect_args(subparsers.add_parser("reconnect"))
    # add_workflow_args(subparsers.add_parser("run-workflow"))

    args = parser.parse_args()
    cmds[args.command](args)


def add_watch_args(parser):
    parser.add_argument("tid", help="Task id")
    return parser


def add_abort_args(parser):
    parser.add_argument("tid", help="Task id")
    return parser


def add_workflow_args(parser):
    parser.add_argument("workflow")
    parser.add_argument("-i", "--inputs", help="workflow inputs")
    parser.add_argument("-p", "--tools", help="required dependencies")
    parser.add_argument("-e", "--environment", choices=["local", "local-connect", "pmac"], default="local")

    return parser


def add_janis_args(parser):
    parser.add_argument("workflow", help="Run the workflow defined in this file")

    parser.add_argument("-e", "--environment", choices=["local", "local-connect", "pmac"], default="local")
    parser.add_argument("-o", "--task-dir", help="The output directory to which tasks are saved in, defaults to $HOME.")

    parser.add_argument("--validation-reference", help="reference file for validation")
    parser.add_argument("--validation-truth-vcf", help="truthVCF for validation")
    parser.add_argument("--validation-intervals", help="intervals to validate between")
    parser.add_argument("--validation-fields", nargs="+", help="outputs from the workflow to validate")

    # add hints
    for HintType in HINTS:
        if issubclass(HintType, HintEnum):
            print("Adding " + HintType.key())
            parser.add_argument("--hint-" + HintType.key(), choices=HintType.symbols())
        else:
            print("Skipping " + HintType.key())

    return parser


def add_reconnect_args(parser):
    parser.add_argument("tid", help="task-id to reconnect to")
    return parser


def do_version(args):
    print("v0.0.2")


def do_run(args):
    Logger.info("Run the shepherd-shepherd with the CommandLine arguments")
    print(args)
    raise NotImplementedError("This path hasn't been implemented yet, raise an issue.")


def do_watch(args):
    tid = args.tid
    tm = ConfigManager().from_tid(tid)
    tm.resume_if_possible()


def do_abort(args):
    tid = args.tid
    tm = ConfigManager().from_tid(tid)
    tm.abort()


def do_janis(args):
    print(args)


if __name__ == "__main__":
    process_args()
