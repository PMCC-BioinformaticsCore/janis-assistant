import argparse
from dotenv import load_dotenv

load_dotenv()



# from shepherd.engines import Cromwell, CWLTool, SyncTask, AsyncTask, CromwellConfiguration
from shepherd.utils.logger import Logger

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
    parser_run.add_argument("-i", "--inputs", help="workflow inputs")
    parser_run.add_argument("-p", "--tools", help="required dependencies")
    parser_run.add_argument("-e", "--environment", choices=["pmac"], default="pmac")
    parser_run.add_argument("-E", "--engine", choices=["cromwell", "cwltool"], default="cromwell")

    args = parser.parse_args()
    print(args)
    cmds[args.command](args)


def do_version(args):
    print("v0.0.1")


def do_run(args):
    Logger.info("Run the shepherd-shepherd with the CommandLine arguments")
    print(args)
    raise NotImplementedError("This path hasn't been implemented yet, raise an issue.")


if __name__ == "__main__":
    process_args()
