#!/usr/bin/env python3
"""Shared CLI for AA follow-up task management."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from megaton_lib.validation.followups import run_pending_verification_cli


if __name__ == "__main__":
    raise SystemExit(run_pending_verification_cli())
