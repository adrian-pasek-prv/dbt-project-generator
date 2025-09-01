import os

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

def _join_path(root_dir: str, resource_type: str, *parts: str) -> str:
    """Join path parts with root directory and resource type."""
    return os.path.join(root_dir, resource_type, *parts)

def _add_metadata_files(paths: set, root_dir: str, resource_type: str, path_parts: list, prefix: str, metadata_files: list) -> None:
    """Add metadata files to the paths set."""
    for metadata_file in metadata_files:
        paths.add(_join_path(root_dir, resource_type, *path_parts, f"{prefix}{metadata_file}"))

def _get_example_model(model_type: str, source_system: str, domain: str = None) -> str:
    """Generate example model filename."""
    if domain:
        return f"{model_type}_{source_system}_{domain}__example_model.sql"
    return f"{model_type}_{source_system}__example_model.sql"

def create_base_path(
    resource_type: str,
    source_system: str,
    domain: str,
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
                    
                    _add_metadata_files(paths, root_dir, resource_type, base_path, f"_{pref}_{source_system}", metadata_files)
                    
                    if pref != "base":
                        paths.add(_join_path(root_dir, resource_type, layer, source_system, f"_{pref}_{source_system}__sources.yml"))
                    
                    paths.add(_join_path(root_dir, resource_type, *base_path, _get_example_model(pref, source_system)))

            elif layer == "intermediate":
                base_path = [layer, source_system, domain]
                _add_metadata_files(paths, root_dir, resource_type, base_path, f"_{prefix}_{source_system}_{domain}", metadata_files)
                paths.add(_join_path(root_dir, resource_type, layer, source_system, domain, 
                                   _get_example_model(prefix, source_system, domain)))

            elif layer == "marts":
                for model_type in model_types:
                    base_path = [layer, source_system, model_type, domain]
                    _add_metadata_files(paths, root_dir, resource_type, base_path, f"{model_type}_{source_system}_{domain}", metadata_files)
                    paths.add(_join_path(root_dir, resource_type, *base_path, 
                                       _get_example_model(model_type, source_system, domain)))

    elif resource_type == "data_tests":
        for test_type in data_tests_types:
            if test_type == "generic":
                paths.add(_join_path(root_dir, resource_type, test_type, f"_test_{test_type}__docs.md"))
                paths.add(_join_path(root_dir, resource_type, test_type, f"test_{test_type}_example_dates_not_in_the_future.sql"))
            else:
                base_path = [test_type, source_system, domain]
                paths.add(_join_path(root_dir, resource_type, *base_path, f"_test_{test_type}_{source_system}_{domain}__docs.md"))
                paths.add(_join_path(root_dir, resource_type, *base_path, f"test_{test_type}_{source_system}_{domain}__example_test.sql"))

    elif resource_type == "seeds":
        prefix = "seed"
        base_path = [source_system, domain]
        _add_metadata_files(paths, root_dir, resource_type, base_path, f"_{prefix}_{source_system}_{domain}", metadata_files)
        paths.add(_join_path(root_dir, resource_type, *base_path, f"{prefix}_{source_system}_{domain}__example_seed.csv"))

    elif resource_type == "macros":
        prefix = "macro"
        base_path = [source_system, domain]
        _add_metadata_files(paths, root_dir, resource_type, base_path, f"_{prefix}_{source_system}_{domain}", metadata_files)
        paths.add(_join_path(root_dir, resource_type, *base_path, f"{prefix}_{source_system}_{domain}__example_generate_payment_methods.sql"))
        
        _add_metadata_files(paths, root_dir, resource_type, ["utils"], f"_{prefix}_utils", metadata_files)
        paths.add(_join_path(root_dir, resource_type, "utils", f"{prefix}_utils__example_cents_to_dollars.sql"))

    return sorted(paths)