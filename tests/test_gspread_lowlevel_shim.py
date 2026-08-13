"""Shim integrity: megaton_lib.gspread_lowlevel === megaton.gsheet_lowlevel.

Behavior tests for the implementation live in the megaton repo
(tests/test_gsheet_lowlevel_*.py); here we only prove the shim exposes the
same objects so every existing import site keeps working.
"""
import megaton.gsheet_lowlevel as core
from megaton_lib import gspread_lowlevel as shim


def test_all_matches_core():
    assert list(shim.__all__) == list(core.__all__)


def test_public_names_are_identical_objects():
    for name in core.__all__:
        assert getattr(shim, name) is getattr(core, name), name


def test_batch_read_helper_is_exported():
    assert shim.fetch_worksheets_values is core.fetch_worksheets_values


def test_underscore_compat_names_delegate():
    for name in [
        "_QUOTA_FLOOR_WAIT",
        "_RETRYABLE_STATUS_CODES",
        "_RATE_LIMIT_403_TOKENS",
        "_RETRY_ACTIVE",
        "_get_status_code",
        "_is_rate_limit_403",
        "GS_EPOCH",
    ]:
        assert getattr(shim, name) is getattr(core, name), name
