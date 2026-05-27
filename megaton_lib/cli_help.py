"""Shared argparse helpers for self-describing CLIs."""

from __future__ import annotations

import argparse
from collections.abc import Sequence


class MegatonHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    """Show default values while preserving multi-line examples."""


def build_parser(
    *,
    description: str,
    examples: Sequence[str] = (),
    notes: Sequence[str] = (),
    **kwargs,
) -> argparse.ArgumentParser:
    """Create a parser with consistent examples and notes sections."""
    epilog_parts: list[str] = []
    if examples:
        epilog_parts.append("Examples:\n" + "\n\n".join(examples))
    if notes:
        epilog_parts.append("Notes:\n" + "\n".join(f"  - {note}" for note in notes))
    epilog = kwargs.pop("epilog", None)
    if epilog:
        epilog_parts.append(str(epilog))
    return argparse.ArgumentParser(
        description=description,
        formatter_class=MegatonHelpFormatter,
        epilog="\n\n".join(epilog_parts) if epilog_parts else None,
        **kwargs,
    )
