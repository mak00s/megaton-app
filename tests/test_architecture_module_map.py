"""docs/architecture.md must place every megaton_lib module in a tier."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _mapped() -> set[str]:
    text = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")
    section = text[text.index("#### Existing Utilities"):text.index("### Out of Scope")]
    return set(re.findall(r"`([a-z_0-9]+)/?`", section))


def _modules() -> set[str]:
    lib = ROOT / "megaton_lib"
    files = {path.stem for path in lib.glob("*.py") if path.name != "__init__.py"}
    packages = {path.name for path in lib.iterdir() if (path / "__init__.py").is_file()}
    return files | packages


def test_every_module_has_a_tier():
    assert sorted(_modules() - _mapped()) == []


def test_module_map_has_no_stale_names():
    assert sorted(_mapped() - _modules()) == []
