import os
from typing import Optional
from pathlib import Path

ROOT_DIR = "."
RESOURCE_TYPES = ["models", "data_tests", "seeds", "macros"]
MODELING_LAYERS = {
    "intermediate": "int",
    "staging": ["stg", "base"],
    "marts": "mart",
    "seeds": "seed",
}
MODEL_TYPES = ["fct", "dim", "mart"]
DATA_TESTS_TYPES = ["generic", "singular"]
METADATA_FILES = ["__schema.yml", "__docs.md"]


def join_path(root_dir: str, resource_type: str, *parts: str) -> str:
    """Join path parts with root directory and resource type."""
    return os.path.join(root_dir, resource_type, *parts)


def add_metadata_files(
    paths: set,
    root_dir: str,
    resource_type: str,
    path_parts: list,
    prefix: str,
    metadata_files: list,
) -> None:
    """Add metadata files to the paths set."""
    for metadata_file in metadata_files:
        paths.add(
            join_path(root_dir, resource_type, *path_parts, f"{prefix}{metadata_file}")
        )


def get_example_model(
    model_type: str,
    source_system: str = None,
    organization: Optional[str] = None,
    domain: str = None,
) -> str:
    """Generate example model filename."""
    parts = [model_type]
    if organization:
        parts.append(organization)
    elif source_system:
        parts.append(source_system)
    if domain:
        parts.append(domain)
    return f"{'_'.join(parts)}__example_model.sql"


def create_base_path(
    resource_type: str,
    source_system: str,
    domain: str,
    organization: Optional[str] = None,
    root_dir: str = ROOT_DIR,
    modeling_layers: dict = MODELING_LAYERS,
    model_types: list = MODEL_TYPES,
    data_tests_types: list = DATA_TESTS_TYPES,
    metadata_files: list = METADATA_FILES,
):
    """Create base paths for dbt project structure based on resource type."""
    paths = set()

    if resource_type == "models":
        for layer, prefix in modeling_layers.items():
            if layer == "staging":
                for pref in prefix:
                    base_path = [layer, source_system]
                    if pref == "base":
                        base_path.append(pref)

                    add_metadata_files(
                        paths,
                        root_dir,
                        resource_type,
                        base_path,
                        f"_{pref}_{source_system}",
                        metadata_files,
                    )

                    if pref != "base":
                        paths.add(
                            join_path(
                                root_dir,
                                resource_type,
                                layer,
                                source_system,
                                f"_{pref}_{source_system}__sources.yml",
                            )
                        )

                    paths.add(
                        join_path(
                            root_dir,
                            resource_type,
                            *base_path,
                            get_example_model(pref, source_system=source_system),
                        )
                    )

            elif layer == "intermediate":
                base_path = [layer]
                base_path.extend([organization, domain] if organization else [domain])
                name_prefix = (
                    f"_{prefix}_{organization}_{domain}"
                    if organization
                    else f"_{prefix}_{domain}"
                )
                add_metadata_files(
                    paths,
                    root_dir,
                    resource_type,
                    base_path,
                    name_prefix,
                    metadata_files,
                )
                paths.add(
                    join_path(
                        root_dir,
                        resource_type,
                        *base_path,
                        get_example_model(
                            prefix, organization=organization, domain=domain
                        ),
                    )
                )

            elif layer == "marts":
                for model_type in model_types:
                    base_path = [layer]
                    base_path.extend(
                        [organization, model_type, domain]
                        if organization
                        else [model_type, domain]
                    )
                    name_prefix = (
                        f"{model_type}_{organization}_{domain}"
                        if organization
                        else f"{model_type}_{domain}"
                    )
                    add_metadata_files(
                        paths,
                        root_dir,
                        resource_type,
                        base_path,
                        name_prefix,
                        metadata_files,
                    )
                    paths.add(
                        join_path(
                            root_dir,
                            resource_type,
                            *base_path,
                            get_example_model(
                                model_type, organization=organization, domain=domain
                            ),
                        )
                    )

    elif resource_type == "data_tests":
        for test_type in data_tests_types:
            if test_type == "generic":
                paths.add(
                    join_path(
                        root_dir,
                        resource_type,
                        test_type,
                        f"_test_{test_type}__docs.md",
                    )
                )
                paths.add(
                    join_path(
                        root_dir,
                        resource_type,
                        test_type,
                        f"test_{test_type}_example_dates_not_in_the_future.sql",
                    )
                )
            else:
                base_path = [test_type]
                base_path.extend([organization, domain] if organization else [domain])
                name_parts = (
                    [test_type, organization, domain]
                    if organization
                    else [test_type, domain]
                )
                paths.add(
                    join_path(
                        root_dir,
                        resource_type,
                        *base_path,
                        f"_test_{'_'.join(name_parts)}__docs.md",
                    )
                )
                paths.add(
                    join_path(
                        root_dir,
                        resource_type,
                        *base_path,
                        f"test_{'_'.join(name_parts)}__example_test.sql",
                    )
                )

    elif resource_type == "seeds":
        prefix = "seed"
        base_path = [organization, domain] if organization else [domain]
        name_prefix = (
            f"_{prefix}_{organization}_{domain}"
            if organization
            else f"_{prefix}_{domain}"
        )
        add_metadata_files(
            paths, root_dir, resource_type, base_path, name_prefix, metadata_files
        )
        name_parts = (
            [prefix, organization, domain] if organization else [prefix, domain]
        )
        paths.add(
            join_path(
                root_dir,
                resource_type,
                *base_path,
                f"{'_'.join(name_parts)}__example_seed.csv",
            )
        )

    elif resource_type == "macros":
        prefix = "macro"
        base_path = [organization, domain] if organization else [domain]
        name_prefix = (
            f"_{prefix}_{organization}_{domain}"
            if organization
            else f"_{prefix}_{domain}"
        )
        add_metadata_files(
            paths, root_dir, resource_type, base_path, name_prefix, metadata_files
        )
        name_parts = (
            [prefix, organization, domain] if organization else [prefix, domain]
        )
        paths.add(
            join_path(
                root_dir,
                resource_type,
                *base_path,
                f"{'_'.join(name_parts)}__example_generate_payment_methods.sql",
            )
        )

        add_metadata_files(
            paths,
            root_dir,
            resource_type,
            ["utils"],
            f"_{prefix}_utils",
            metadata_files,
        )
        paths.add(
            join_path(
                root_dir,
                resource_type,
                "utils",
                f"{prefix}_utils__example_cents_to_dollars.sql",
            )
        )

        # Add utilities at the root
        utilities = ["dbt_project.yml", "README.md", "analyses", "packages.yml"]
        for utility in utilities:
            paths.add(os.path.join(root_dir, utility))
    return sorted(paths)


def generate_project_paths(
    root_dir: str = ROOT_DIR,
    resource_types: list = RESOURCE_TYPES,
    source_systems: list = ["stripe", "shopify"],
    organizations: Optional[list] = None,
    domains: list = ["sales", "marketing", "core"],
    modeling_layers: dict = MODELING_LAYERS,
    model_types: list = MODEL_TYPES,
    data_tests_types: list = DATA_TESTS_TYPES,
    metadata_files: list = METADATA_FILES,
):
    """Generate all project paths for the dbt project structure."""
    all_paths = set()
    for resource_type in resource_types:
        for source_system in source_systems:
            if organizations:
                for organization in organizations:
                    for domain in domains:
                        paths = create_base_path(
                            resource_type,
                            source_system,
                            domain,
                            organization,
                            root_dir,
                            modeling_layers,
                            model_types,
                            data_tests_types,
                            metadata_files,
                        )
                        all_paths.update(paths)
            else:
                for domain in domains:
                    paths = create_base_path(
                        resource_type,
                        source_system,
                        domain,
                        None,
                        root_dir,
                        modeling_layers,
                        model_types,
                        data_tests_types,
                        metadata_files,
                    )
                    all_paths.update(paths)
    return sorted(all_paths)

def create_project_structure(paths: set, print_only: bool = True) -> None:
    """Create project structure based on the provided paths"""
    for path in paths:
        if print_only:
            print(path)
            continue
        # Create a Path object
        file_path = Path(path)
        # Create parent directories if they don't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        # Touch empty file at the end of path if it contains a file extension
        # otherwise create the directory
        if file_path.suffix:
            file_path.touch(exist_ok=True)
        else:
            file_path.mkdir(exist_ok=True, )

