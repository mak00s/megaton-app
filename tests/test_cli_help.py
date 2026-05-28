from __future__ import annotations

import pytest

from megaton_lib.cli_help import build_parser


def _help_text(parser) -> str:
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--help"])
    assert exc_info.value.code == 0
    return parser.format_help()


def test_build_parser_shows_defaults() -> None:
    parser = build_parser(description="Demo CLI")
    parser.add_argument("--limit", type=int, default=20, help="row limit")

    help_text = _help_text(parser)

    assert "--limit LIMIT" in help_text
    assert "row limit (default: 20)" in help_text


def test_build_parser_preserves_multiline_examples() -> None:
    parser = build_parser(
        description="Demo CLI",
        examples=[
            "python demo.py --one",
            "python demo.py \\\n  --two 2",
        ],
    )

    help_text = _help_text(parser)

    assert "Examples:" in help_text
    assert "python demo.py --one" in help_text
    assert "python demo.py \\\n  --two 2" in help_text


def test_build_parser_merges_notes_and_existing_epilog() -> None:
    parser = build_parser(
        description="Demo CLI",
        notes=["Exit code 0 means ok."],
        epilog="More details.",
    )

    help_text = _help_text(parser)

    assert "Notes:" in help_text
    assert "  - Exit code 0 means ok." in help_text
    assert "More details." in help_text
