"""Tag configuration providers."""

from .adobe_tags import (
    extract_mapping_from_settings,
    fetch_adobe_tags_mapping,
    list_data_elements,
    parse_settings_object,
)
from .gtm import fetch_gtm_mapping, parse_regex_table_variable
from .sync import apply_custom_code_tree, find_component_id, slugify_component_name

__all__ = [
    "extract_mapping_from_settings",
    "fetch_adobe_tags_mapping",
    "fetch_gtm_mapping",
    "list_data_elements",
    "parse_regex_table_variable",
    "parse_settings_object",
    "apply_custom_code_tree",
    "find_component_id",
    "slugify_component_name",
]
