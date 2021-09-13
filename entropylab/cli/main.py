import argparse
import functools
import sys

from entropylab.logger import logger
from entropylab.results import serve_results
from entropylab.results_backend.sqlalchemy import init_db, upgrade_db


# decorator


def command(func: callable) -> callable:
    """Decorator that runs commands. On error, prints friendly message when possible """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except RuntimeError as re:
            command_name = func.__name__
            logger.exception(
                "RuntimeError in Entropy CLI command %s, args: %s", command_name, args
            )
            print(re, file=sys.stderr)
            sys.exit(-1)

    return wrapper


# CLI command functions


@command
def init(args: argparse.Namespace):
    init_db(args.directory)


@command
def update(args: argparse.Namespace):
    upgrade_db(args.directory)


@command
def serve(args: argparse.Namespace):
    serve_results(args.directory, port=args.port)


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
