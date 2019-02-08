import argparse
from utils.logger import Logger

__version__ = "v0.0.1"


def process_args():

    cmds = {
        "version": do_version,
        "run": do_run

    }

    parser = argparse.ArgumentParser(description="Execute a workflow")
    subparsers = parser.add_subparsers(help="subcommand help", dest="command")
    parser.add_argument("-d", "--debug", action="store_true")

    parser_version = subparsers.add_parser("version")

    parser_run = subparsers.add_parser("run")
    parser_run.add_argument("workflow")

    args = parser.parse_args()
    print(args)
    cmds[args.command](args)


def do_version(args):
    print("v0.0.1")


def do_run(args):
    Logger.info("Run the shepherd-runner with the CommandLine arguments")
    print(args)


if __name__ == "__main__":
    process_args()
