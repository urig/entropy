import argparse
import sys
import traceback

from entropylab.logger import logger
from entropylab.results import serve_results
from entropylab.results_backend.sqlalchemy import init_db, upgrade_db


# CLI command functions


def init(args: argparse.Namespace):
    _safe_run_command(init_db, args.directory)


def serve(args: argparse.Namespace):
    _safe_run_command(serve_results, args.directory, port=args.port)


def update(args: argparse.Namespace):
    _safe_run_command(upgrade_db, args.directory)


# noinspection PyBroadException
def _safe_run_command(command_func: callable, *args, **kwargs) -> None:
    try:
        command_func(*args, **kwargs)
    except Exception:
        command_name = command_func.__name__
        logger.exception(
            f"Entropy CLI command %s raised an error. args: %s", command_name, args
        )
        traceback.print_exc()
        sys.exit(-1)


def _build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    directory_arg = {
        "help": "path to a directory containing Entropy project",
        "nargs": "?",
        "default": ".",
    }

    # init
    init_parser = subparsers.add_parser("init", help="initialize a new Entropy project")
    init_parser.add_argument("directory", **directory_arg)
    init_parser.set_defaults(func=init)

    # update
    update_parser = subparsers.add_parser(
        "update", help="update an Entropy project to the latest version"
    )
    update_parser.add_argument("directory", **directory_arg)
    update_parser.set_defaults(func=update)

    # serve
    serve_parser = subparsers.add_parser(
        "serve",
        help="launch results server in a new browser window",
    )
    serve_parser.add_argument("directory", **directory_arg)
    serve_parser.add_argument("--port", type=int, default=0)
    serve_parser.set_defaults(func=serve)

    return parser


# main


def main():
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
