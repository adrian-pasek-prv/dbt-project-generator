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

args = parser.parse_args()

print(args)

paths = generate_project_paths(
    root_dir="./dbt_core_project",
    source_systems=["single_platform", "gecad"],
    domains=["merchant_cash", "core"]
)


# create_project_structure(paths, print_only=False)