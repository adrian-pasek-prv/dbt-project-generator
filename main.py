#!/usr/bin/env python3

import argparse
from utils import *

# Initialize parser
msg = "This tool allows to create a template for a dbt project with\
    preconfigured sources and data products"
parser = argparse.ArgumentParser(description=msg)

parser.add_argument(
    "--root-dir",
    type=str,
    help="Root directory of a project",
)
parser.add_argument(
    "--source-systems",
    type=list_of_strings,
    help="Comma-separated list of source systems",
)
parser.add_argument(
    "--domains",
    type=list_of_strings,
    help="Comma-separated list of domains",
)

parser.add_argument(
    "--organizations",
    type=list_of_strings,
    required=False,
    help="Comma-separated list of organizations",
)

parser.add_argument(
    "--print-only",
    action='store_true',
    help="Print paths in the terminal"
)

args = parser.parse_args()

if __name__ == "__main__":

    paths = generate_project_paths(
        root_dir=args.root_dir,
        source_systems=args.source_systems,
        domains=args.domains,
    )

    create_project_structure(paths,
                             print_only=args.print_only)