"""Tag configuration providers."""

from .bootstrap import adobe_tags_output_root, build_tags_config, load_env_file, merge_adobe_scopes, seed_adobe_oauth_env
from .cli import gtm_export_main, tags_apply_main, tags_export_main
from .build_workflow import collect_changed_resources, run_build_workflow, verify_build_markers
from .adobe_tags import (
    export_property,
    extract_mapping_from_settings,
    fetch_adobe_tags_mapping,
    list_data_elements,
    parse_settings_object,
    sync_property,
)
from .gtm import export_container, fetch_gtm_mapping, parse_regex_table_variable, sync_container
from .sync import apply_custom_code_tree, find_component_id, slugify_component_name

__all__ = [
    "adobe_tags_output_root",
    "build_tags_config",
    "collect_changed_resources",
    "export_container",
    "export_property",
    "extract_mapping_from_settings",
    "fetch_adobe_tags_mapping",
    "fetch_gtm_mapping",
    "gtm_export_main",
    "list_data_elements",
    "load_env_file",
    "merge_adobe_scopes",
    "parse_regex_table_variable",
    "sync_container",
    "sync_property",
    "parse_settings_object",
    "apply_custom_code_tree",
    "find_component_id",
    "seed_adobe_oauth_env",
    "slugify_component_name",
    "run_build_workflow",
    "tags_apply_main",
    "tags_export_main",
    "verify_build_markers",
]
