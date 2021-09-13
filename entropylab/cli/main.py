import argparse
import os

from entropylab.results import serve_results
from entropylab.results_backend.sqlalchemy import upgrade_db


def results_function(args: argparse.Namespace):
    serve_results(args.path)


def database_function(args: argparse.Namespace):
    upgrade_db(args.path)


def build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    # results
    results_parser = subparsers.add_parser(
        "results", help="view and chart experiment results"
    )
    results_parser.add_argument(
        "action",
        choices=["serve"],
        help="start entropy results viewer in a new browser window",
    )
    results_parser.add_argument("path", help="path to database")
    results_parser.set_defaults(func=results_function)

    # database
    database_parser = subparsers.add_parser("database", help="database maintenance")
    database_parser.add_argument(
        "action",
        choices=["update"],
        help="maintenance action to perform on the database",
    )
    database_parser.add_argument("path", help="path to database")
    database_parser.set_defaults(func=database_function)

    return parser


def main():
    print("CWD: " + os.getcwd())
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
