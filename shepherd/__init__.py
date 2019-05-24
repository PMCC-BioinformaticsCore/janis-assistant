import argparse
from dotenv import load_dotenv

from shepherd.management.configmanager import ConfigManager

load_dotenv()

from shepherd.engines import Cromwell, CWLTool, CromwellConfiguration
from shepherd.management.taskmanager import TaskManager, TaskStatus
from shepherd.environments.environment import Environment
from shepherd.utils.logger import Logger
from shepherd.validation import ValidationRequirements

__version__ = "v0.0.1"


def process_args():

    cmds = {
        "version": do_version,
        "janis": print,
        "run": do_run,
        "watch": do_watch
    }

    parser = argparse.ArgumentParser(description="Execute a workflow")
    subparsers = parser.add_subparsers(help="subcommand help", dest="command")
    parser.add_argument("-d", "--debug", action="store_true")

    subparsers.add_parser("version")
    add_watch_args(subparsers.add_parser("watch"))
    # add_abort_args(subparsers.add_parser("abort"))
    add_janis_args(subparsers.add_parser("janis"))
    add_reconnect_args(subparsers.add_parser("reconnect"))
    # add_workflow_args(subparsers.add_parser("run-workflow"))

    args = parser.parse_args()
    print(args)
    cmds[args.command](args)


def add_watch_args(parser):
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
    parser.add_argument("-t", "--task-dir", help="The output directory to which tasks are saved in, defaults to $HOME.")
    parser.add_argument("-r", "--reference", help="reference file for validation")
    # parser.add_argument("-t", "--truth-vcf", help="truthVCF for validation")

    # parser.add_argument("args", nargs=argparse.REMAINDER)

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

    ConfigManager().from_tid(tid)


if __name__ == "__main__":
    process_args()
