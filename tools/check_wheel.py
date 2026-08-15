"""Verify the installable boundary of a built megaton-app wheel."""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath
from zipfile import ZipFile


def verify_wheel(path: Path) -> None:
    if not path.is_file() or path.suffix != ".whl":
        raise SystemExit(f"expected one wheel path, got: {path}")

    with ZipFile(path) as archive:
        names = [PurePosixPath(name) for name in archive.namelist()]

    if not any(name.parts[0] == "megaton_lib" for name in names):
        raise SystemExit("wheel does not contain megaton_lib")

    dist_info_roots = {
        name.parts[0]
        for name in names
        if name.parts and name.parts[0].endswith(".dist-info")
    }
    if len(dist_info_roots) != 1:
        raise SystemExit(f"expected one .dist-info directory, got: {dist_info_roots}")

    allowed_roots = {"megaton_lib", *dist_info_roots}
    unexpected = sorted(
        str(name) for name in names if name.parts and name.parts[0] not in allowed_roots
    )
    if unexpected:
        raise SystemExit(f"unexpected wheel contents: {unexpected}")

    forbidden_roots = {"app", "scripts", "tests", "configs", "credentials", "input", "output"}
    included_forbidden = sorted(
        str(name) for name in names if name.parts and name.parts[0] in forbidden_roots
    )
    if included_forbidden:
        raise SystemExit(f"checkout-local content found in wheel: {included_forbidden}")

    if not any(name.name == "LICENSE" for name in names):
        raise SystemExit("wheel does not contain LICENSE")

    print(f"verified {path}: {len(names)} files, installable package megaton_lib only")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    verify_wheel(args.wheel)


if __name__ == "__main__":
    main()
