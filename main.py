#!/usr/bin/env python3

from utils import *

paths = generate_project_paths(
    root_dir="./dbt_core_project",
    source_systems=["single_platform", "gecad"],
    organizations=["payupoland", "payuromania"],
    domains=["merchant_cash", "core"]
)

for path in paths:
    print(path)