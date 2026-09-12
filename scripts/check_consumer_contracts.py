"""Run explicitly selected consumer tests against an installed candidate library.

Install a released tag or candidate wheel into a separate --library-path first.
This command does not install dependencies, publish, or invoke report workflows.
Only select offline tests: pytest executes code supplied by the consumer repo.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path, help="Consumer checkout or isolated staged snapshot")
    parser.add_argument("--library-path", required=True, type=Path, help="pip --target directory containing megaton_lib")
    parser.add_argument("--test", action="append", required=True, help="Offline pytest file relative to repo; repeatable")
    args = parser.parse_args(argv)
    repo = args.repo.resolve()
    library = args.library_path.resolve()
    if not repo.is_dir() or not (library / "megaton_lib" / "__init__.py").is_file():
        parser.error("repo and installed megaton_lib directory must exist")
    tests = []
    for value in args.test:
        path = (repo / value).resolve()
        if not path.is_relative_to(repo) or not path.is_file() or path.suffix != ".py":
            parser.error(f"test must be a Python file inside repo: {value}")
        tests.append(str(path))
    env = dict(os.environ, PYTHONPATH=str(library))
    # Verify the import from inside the pytest process, including conftest effects.
    runner = '''
import pathlib, sys, pytest
expected = pathlib.Path(sys.argv[1]).resolve()
class VerifyLibrary:
    def pytest_collection_finish(self, session):
        import megaton_lib
        actual = pathlib.Path(megaton_lib.__file__).resolve()
        if not actual.is_relative_to(expected):
            raise pytest.UsageError(f"Wrong megaton_lib loaded: {actual}; expected {expected}")
        print(f"consumer_library={actual}")
raise SystemExit(pytest.main(["-q", *sys.argv[2:]], plugins=[VerifyLibrary()]))
'''
    return subprocess.run(
        [sys.executable, "-c", runner, str(library), *tests], cwd=repo, env=env, check=False,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
