"""Run a Jupytext percent-format notebook from the CLI.

Variables in the parameters cell (``tags=["parameters"]``) can be overridden
from the command line. This supports scheduled execution in GitHub Actions.
matplotlib uses the Agg backend so ``plt.show()`` has no GUI effect.

Usage:
    python scripts/run_notebook.py <notebook.py> [-p KEY=VALUE ...]

Examples:
    # Run with default parameters
    python scripts/run_notebook.py notebooks/reports/yokohama_cv.py

    # Override with date templates
    python scripts/run_notebook.py notebooks/reports/yokohama_cv.py \\
      -p START_DATE=today-30d -p END_DATE=today
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# --- Cell parsing --------------------------------------------------------

_CELL_MARKER = re.compile(r"^# %%(.*)$")
_PARAMS_TAG = re.compile(r'tags\s*=\s*\[.*"parameters".*\]')
_HEADER_END = re.compile(r"^# ---\s*$")


def extract_cells(source: str) -> list[dict]:
    """Split a Jupytext percent-format ``.py`` into a list of cells.

    Returns:
        list of {"marker": str, "source": str, "is_params": bool}
        Excludes the leading YAML header section (bounded by ``# ---``).
    """
    lines = source.splitlines(keepends=True)
    cells: list[dict] = []

    # --- Skip YAML header ---
    i = 0
    if lines and lines[0].strip() == "# ---":
        i = 1
        while i < len(lines):
            if _HEADER_END.match(lines[i].strip()) and i > 0:
                i += 1
                break
            i += 1

    # --- Split into cells ---
    current_marker = ""
    current_lines: list[str] = []

    while i < len(lines):
        m = _CELL_MARKER.match(lines[i].rstrip())
        if m:
            # Flush previous cell
            if current_marker or current_lines:
                cells.append(_make_cell(current_marker, current_lines))
            current_marker = lines[i].rstrip()
            current_lines = []
        else:
            current_lines.append(lines[i])
        i += 1

    # Last cell
    if current_marker or current_lines:
        cells.append(_make_cell(current_marker, current_lines))

    return cells


def _make_cell(marker: str, lines: list[str]) -> dict:
    source = "".join(lines)
    is_params = bool(_PARAMS_TAG.search(marker))
    return {"marker": marker, "source": source, "is_params": is_params}


# --- Parameter injection -------------------------------------------------

_ASSIGN_RE = re.compile(r"^([A-Z_][A-Z0-9_]*)\s*=\s*(.+)$", re.MULTILINE)


def _resolve_value(raw: str) -> str:
    """Resolve date template values; return raw value on failure."""
    try:
        from megaton_lib.date_template import resolve_date
        return resolve_date(raw)
    except (ValueError, ImportError):
        return raw


def _format_value(value: str) -> str:
    """Format a value as a Python literal.

    Numeric values are kept as-is; everything else is safely quoted.
    """
    # int / float detection
    try:
        int(value)
        return value
    except ValueError:
        pass
    try:
        float(value)
        return value
    except ValueError:
        pass
    # String (safely escaped)
    return repr(value)


def inject_params(cells: list[dict], overrides: dict[str, str]) -> list[dict]:
    """Override variables in the parameters cell with ``overrides``.

    Date templates (e.g. ``today-7d``) are resolved automatically.
    Keys not present in the cell are ignored.
    """
    if not overrides:
        return cells

    result = []
    for cell in cells:
        if not cell["is_params"]:
            result.append(cell)
            continue

        new_source = cell["source"]
        for key, raw_value in overrides.items():
            resolved = _resolve_value(raw_value)
            formatted = _format_value(resolved)
            # Replace `KEY = ...` line
            pattern = re.compile(
                rf"^({re.escape(key)}\s*=\s*)(.+)$", re.MULTILINE
            )
            new_source = pattern.sub(rf"\g<1>{formatted}", new_source)

        result.append({**cell, "source": new_source})
    return result


# --- Execution -----------------------------------------------------------


def run(notebook_path: str, overrides: dict[str, str]) -> None:
    """Execute the notebook as a script."""
    os.environ.setdefault("MPLBACKEND", "Agg")

    nb = Path(notebook_path).resolve()
    if not nb.exists():
        raise FileNotFoundError(f"Notebook not found: {nb}")

    source = nb.read_text(encoding="utf-8")
    cells = extract_cells(source)
    cells = inject_params(cells, overrides)

    # Build script by excluding markdown cells
    code_parts = []
    for cell in cells:
        if "# %% [markdown]" in cell["marker"]:
            continue
        code_parts.append(cell["source"])

    script = "\n".join(code_parts)

    # Set CWD to the notebook directory
    original_cwd = os.getcwd()
    os.chdir(nb.parent)
    try:
        compiled = compile(script, str(nb), "exec")
        exec_globals = {"__name__": "__main__", "__file__": str(nb)}
        exec(compiled, exec_globals)
    finally:
        os.chdir(original_cwd)


# --- CLI -----------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a jupytext percent-format notebook from CLI",
    )
    parser.add_argument("notebook", help="Path to .py notebook")
    parser.add_argument(
        "-p", "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override a parameter (repeatable). Date templates supported.",
    )
    return parser.parse_args(argv)


def _parse_param_pairs(params: list[str]) -> dict[str, str]:
    """Convert a list like ``['-p', 'K=V', ...]`` into a dict."""
    result = {}
    for p in params:
        if "=" not in p:
            raise ValueError(f"Invalid param format (expected KEY=VALUE): {p}")
        key, value = p.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        overrides = _parse_param_pairs(args.param)
        run(args.notebook, overrides)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
