"""Tag configuration providers."""

from .adobe_tags import (
    extract_mapping_from_settings,
    fetch_adobe_tags_mapping,
    list_data_elements,
    parse_settings_object,
)
from .gtm import fetch_gtm_mapping, parse_regex_table_variable

__all__ = [
    "extract_mapping_from_settings",
    "fetch_adobe_tags_mapping",
    "fetch_gtm_mapping",
    "list_data_elements",
    "parse_regex_table_variable",
    "parse_settings_object",
]
