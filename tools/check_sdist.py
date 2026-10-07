"""Verify the source distribution boundary for megaton-app."""

from __future__ import annotations

import argparse
import tarfile
from pathlib import Path, PurePosixPath


def verify_sdist(path: Path) -> None:
    if not path.is_file() or not path.name.endswith(".tar.gz"):
        raise SystemExit(f"expected one .tar.gz path, got: {path}")

    with tarfile.open(path, "r:gz") as archive:
        names = [PurePosixPath(member.name) for member in archive.getmembers()]

    roots = {name.parts[0] for name in names if name.parts}
    if len(roots) != 1:
        raise SystemExit(f"expected one archive root, got: {roots}")

    archive_root = next(iter(roots))
    relative = [PurePosixPath(*name.parts[1:]) for name in names if len(name.parts) > 1]
    if not any(name.parts and name.parts[0] == "megaton_lib" for name in relative):
        raise SystemExit("sdist does not contain megaton_lib")

    forbidden_roots = {"app", "scripts", "tests", "configs", "credentials", "input", "output"}
    included_forbidden = sorted(
        str(name) for name in relative if name.parts and name.parts[0] in forbidden_roots
    )
    if included_forbidden:
        raise SystemExit(f"checkout-local content found in sdist: {included_forbidden}")

    metadata_roots = {
        name.parts[0]
        for name in relative
        if name.parts and name.parts[0].endswith(".egg-info")
    }
    allowed_roots = {
        "LICENSE",
        "MANIFEST.in",
        "PKG-INFO",
        "README.md",
        "megaton_lib",
        "pyproject.toml",
        "setup.cfg",
        *metadata_roots,
    }
    unexpected = sorted(
        str(name) for name in relative if name.parts and name.parts[0] not in allowed_roots
    )
    if unexpected:
        raise SystemExit(f"unexpected sdist contents: {unexpected}")

    print(f"verified {path}: archive root {archive_root}, no checkout-local content")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("sdist", type=Path)
    args = parser.parse_args()
    verify_sdist(args.sdist)


if __name__ == "__main__":
    main()
