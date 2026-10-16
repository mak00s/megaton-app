"""Install fail-closed local hygiene hooks without replacing custom hooks."""
from __future__ import annotations

import subprocess
from pathlib import Path


def install() -> None:
    root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
    current = subprocess.run(["git", "config", "--get", "core.hooksPath"],
                             capture_output=True, text=True).stdout.strip()
    if current and current != ".githooks":
        raise SystemExit("Custom core.hooksPath exists; integrate hooks manually, do not overwrite it.")
    if not current:
        for name in ("pre-commit", "pre-push"):
            path = Path(subprocess.check_output(["git", "rev-parse", "--git-path", f"hooks/{name}"], text=True).strip())
            if path.exists():
                raise SystemExit("Existing hook found; integrate hygiene checks manually.")
    for name in ("pre-commit", "pre-push"):
        path = root / ".githooks" / name
        if not path.is_file():
            raise SystemExit("Missing tracked hook template; no config was changed.")
    for name in ("pre-commit", "pre-push"):
        path = root / ".githooks" / name
        path.chmod(path.stat().st_mode | 0o111)
    subprocess.run(["git", "config", "--local", "core.hooksPath", ".githooks"], check=True)
    print("Installed pre-commit (index) and pre-push (full history) hygiene gates. A valid local key is required.")


if __name__ == "__main__":
    install()
