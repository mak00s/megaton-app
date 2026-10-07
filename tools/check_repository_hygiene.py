"""Reject known private identifiers and high-confidence secrets.

Default mode checks tracked files (paths and contents). ``--history`` also
checks every commit reachable from any ref: file versions, paths, commit
messages, author/committer identities and annotated tag messages. Findings
never print the matched identifier, only where it was found.

The denylist is stored as HMAC-SHA256 values under a private key, because a
plain hash of a short word is recovered by brute force in well under a second.
The key comes from ``REPOSITORY_HYGIENE_KEY`` (CI secret), from the file named
by ``REPOSITORY_HYGIENE_KEY_FILE``, or from
``~/.config/megaton-app/repository-hygiene.key``. Without the key the
identifier checks cannot run; secret-pattern checks need no key.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import re
import subprocess
from pathlib import Path


KEY_ENV = "REPOSITORY_HYGIENE_KEY"
KEY_FILE_ENV = "REPOSITORY_HYGIENE_KEY_FILE"
DEFAULT_KEY_FILE = Path.home() / ".config" / "megaton-app" / "repository-hygiene.key"
# Detects a wrong or truncated key, which would otherwise pass everything.
KEY_CHECK_TEXT = "megaton-app repository hygiene key check"
KEY_CHECK_MAC = "b05088f0b803ae89304adcb99a3de8b68cef171d5cb1cf5fa182de4040b52c7c"

# HMAC-SHA256(key, casefolded identifier). Useless for recovery without the key.
FORBIDDEN_TOKEN_MACS = {
    "1965cb59d33e97d4b9c684451d9701955196281a26b56936b54398cd63da1529",
    "204e9642837faf64273a0e23778a353da5887cd24f9548663fc083b122d14f16",
    "21110518ebd9c1e401f2f28ebbc039e0fc7e37afc1649b8c1fcce6096606725c",
    "22feb4488e5f90b2e13a556f545365a47059afa26859cedf8f841c60f65164f7",
    "29258bd05161c922898e81c7894abbb00ee0e36a08fe65c107e657a8b5a1b65d",
    "322c77780cadd02f1426433aba328a9a9a358fcd6e41832bc852156d2aba9c64",
    "347cfd1ef2d4a53d9d9d30bd1ca815be477579dcf3ba8b48b68315e6039ffe4c",
    "4046102b87d92a16b8e89c8a1d277e9a8305c8b91d859c4a3f6adba90a165c78",
    "429ad1f2401348d57fd2726a41c2bd1ff65189b73a4ebff245e6472e457cbaff",
    "49012ebd3a963007cf56d8fc15c3016ce5a859e59cbcb358985359c6f8ec888e",
    "4ddccf5ed79d2301e3812b9b169749730e882f8bbd88c0ba17d30ebec42a4574",
    "4dfd64983be37004a77a33b91daba36b8064c5d3f267c849ddbbf559d9faff4c",
    "7b118cf391e5d94348a47d661e088efb8049e687b234a7bf5933c7407e9eaf94",
    "81ea5e82b1a026d1aa3d67c97ac635954b364760c164f1080ffcf946e971b360",
    "8914f15d92320775b34ed5d2153f97dad9ff3cac452952646933d33edc5d27b0",
    "965fc9f68cbd379f8f442a117c896c93d4b45d83b8763a2ed186598794db7dc0",
    "96fad78664922caecd018827e60bfc521aefaba21e2970a13ed0cd92b30e7903",
    "98dd5c4b89746a7b7d659b27c8b1343633249952a7e43dfb819be1f0ed5232c2",
    "9f19f5638892e09e068db0f34a218b9665983fa1cee78ecea59209a0eeaf5fee",
    "ab4abc25070fb9fe2cb20929783f518b70a94fb1ab6e85e8c777867c1ef41fdc",
    "abe50e746fcef6d110a93574a3cbe11b21540c9a1a69154d26cb5255f902d22a",
    "b91f91c52ea5354f24993d03ff96008f0b470b48cd5e98113d3ea1b8d1088193",
    "bb538516e751c50d634c08106863028abc3db531cfbc0470ea6daf0bd0b74005",
    "c2f9a9b56ac1b5fdd7cc6e555edfd6acfb2a1c01453662405b1736d572ddac33",
    "c889695567a20792b771f3603c03098900c8627bb8eeee05321dee60d735b1b5",
    "d7182695bf0003bb5453132339bf1d9941c08bd74729958d5bb76ebc1df45116",
    "dc3ab6ddbf6bbd4427d18dd299970795fe05b16e3cdcb130451047c55848ed3d",
    "e0aa24f5cc10f44abc15642a6f046da7753cc4ba7c51f552d27656d168d60799",
    "f9b12e28a63bdab8cc699c934b930a37832d424e914c2d995eff251878d6cee3",
}

TOKEN_RE = re.compile(r"[\w@.-]+", re.UNICODE)
SEPARATOR_RE = re.compile(r"([._@-])")
SECRET_PATTERNS = {
    "Google API key": re.compile(r"AIza[0-9A-Za-z_-]{30,}"),
    "GitHub token": re.compile(r"(?:github_pat_|ghp_)[0-9A-Za-z_]{20,}"),
    "Slack token": re.compile(r"xox[baprs]-[0-9A-Za-z-]{10,}"),
    "OpenAI-style key": re.compile(r"sk-[0-9A-Za-z]{30,}"),
    "private key": re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----[\s\S]{100,}?"
        r"-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
    ),
}

_TOKEN_CACHE: dict[str, bool] = {}
_KEY: bytes | None = None


def _token_mac(token: str, key: bytes | None = None) -> str:
    key = _KEY if key is None else key
    if key is None:
        raise RuntimeError("hygiene key is not loaded")
    return hmac.new(key, token.casefold().encode(), hashlib.sha256).hexdigest()


def load_key() -> bytes | None:
    """Return the key from the environment or a key file; None when absent."""
    value = os.environ.get(KEY_ENV, "").strip()
    if not value:
        path = Path(os.environ.get(KEY_FILE_ENV, "") or DEFAULT_KEY_FILE).expanduser()
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
    return value.encode() if value else None


def _is_forbidden(token: str) -> bool:
    """Match the token or any contiguous run of its ``. _ @ -`` separated parts.

    A denylisted word must also be caught inside compounds such as
    ``<word>-analysis``, ``PR-<WORD>`` or ``.env.<word>``, and a denylisted
    domain inside a longer host name. Parts that are merely adjacent to other
    letters or digits (``elapsedMs``) are not split, which avoids false hits.
    """
    if _KEY is None:
        return False
    cached = _TOKEN_CACHE.get(token)
    if cached is not None:
        return cached
    pieces = SEPARATOR_RE.split(token)
    words = range(0, len(pieces), 2)
    result = False
    for start in words:
        if not pieces[start]:
            continue
        for end in words:
            if end < start or not pieces[end]:
                continue
            if _token_mac("".join(pieces[start:end + 1])) in FORBIDDEN_TOKEN_MACS:
                result = True
                break
        if result:
            break
    _TOKEN_CACHE[token] = result
    return result


def _has_forbidden(text: str) -> bool:
    return any(_is_forbidden(token) for token in TOKEN_RE.findall(text))


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args]).decode("utf-8", errors="replace")


def check_tracked_files() -> list[str]:
    findings: list[str] = []
    for value in subprocess.check_output(["git", "ls-files", "-z"]).split(b"\0"):
        if not value:
            continue
        path = Path(value.decode())
        if _has_forbidden(path.as_posix()):
            findings.append(f"{path}: forbidden path")
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for line_number, line in enumerate(text.splitlines(), 1):
            if _has_forbidden(line):
                findings.append(f"{path}:{line_number}: forbidden private identifier")
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                findings.append(f"{path}: possible {label}")
    return findings


def check_history() -> list[str]:
    """Check everything reachable from any ref, not only the checked-out tree."""
    findings: list[str] = []
    marker = "\x1e"
    log = _git("log", "--all", f"--format={marker}%H%n%an <%ae>%n%cn <%ce>%n%B")
    for entry in log.split(marker)[1:]:
        commit, author, committer, *message = entry.split("\n")
        if _has_forbidden(author) or _has_forbidden(committer):
            findings.append(f"{commit[:12]}: forbidden identifier in author/committer identity")
        if _has_forbidden("\n".join(message)):
            findings.append(f"{commit[:12]}: forbidden identifier in commit message")

    tags = _git("for-each-ref", "refs/tags", f"--format={marker}%(refname:short)%00%(taggername) %(taggeremail)%00%(contents)")
    for entry in tags.split(marker)[1:]:
        name, tagger, contents = entry.split("\0", 2)
        if _has_forbidden(name) or _has_forbidden(tagger) or _has_forbidden(contents):
            findings.append(f"tag {name if not _has_forbidden(name) else '<redacted>'}: forbidden identifier in tag")

    commit = ""
    path = ""
    reported: set[tuple[str, str, str]] = set()
    added: dict[tuple[str, str], list[str]] = {}
    patch = _git("log", "--all", "-p", "--no-color", "--full-history", "-m", f"--format={marker}%H")
    # Not splitlines(): it would also break lines on the record marker itself.
    for line in patch.split("\n"):
        if line.startswith(marker):
            commit = line[1:13]
            continue
        if line.startswith("diff --git "):
            path = line.split(" b/", 1)[-1]
            if _has_forbidden(line):
                reported.add((commit, "<redacted path>", "forbidden path"))
            continue
        if line.startswith(("+++", "---")) or not line.startswith(("+", "-")):
            continue
        if _has_forbidden(line[1:]):
            reported.add((commit, path, "forbidden private identifier in file history"))
        if line.startswith("+"):
            added.setdefault((commit, path), []).append(line[1:])
    findings.extend(f"{c}: {p}: {kind}" for c, p, kind in sorted(reported))
    # Join per commit and file so a multi-line secret is seen whole, without
    # stitching unrelated versions together into a false match.
    for (c, p), lines in sorted(added.items()):
        text = "\n".join(lines)
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                findings.append(f"{c}: {p}: possible {label} in a past file version")
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--history",
        action="store_true",
        help="also check all commits, identities, messages and tags (needs a full clone)",
    )
    parser.add_argument(
        "--allow-missing-key",
        action="store_true",
        help="run only the secret-pattern checks when the denylist key is unavailable",
    )
    args = parser.parse_args(argv)

    global _KEY
    _TOKEN_CACHE.clear()
    _KEY = load_key()
    if _KEY is None:
        if not args.allow_missing_key:
            print(
                "Repository hygiene check failed:\n"
                f"- denylist key not found; set {KEY_ENV} or {KEY_FILE_ENV}, "
                "or pass --allow-missing-key to run secret-pattern checks only"
            )
            return 1
        print("Denylist key unavailable: private-identifier checks SKIPPED; secret patterns only")
    elif not hmac.compare_digest(_token_mac(KEY_CHECK_TEXT), KEY_CHECK_MAC):
        print("Repository hygiene check failed:\n- denylist key does not match this repository's denylist")
        return 1
    findings = check_tracked_files()
    if args.history:
        if _git("rev-parse", "--is-shallow-repository").strip() == "true":
            print("Repository hygiene check failed:\n- --history requires a full clone (fetch-depth: 0)")
            return 1
        findings.extend(check_history())

    if findings:
        print("Repository hygiene check failed:")
        print("\n".join(f"- {finding}" for finding in findings))
        return 1
    scope = "tracked files and full history" if args.history else "tracked files"
    coverage = "identifiers and secrets" if _KEY is not None else "secrets only"
    print(f"Repository hygiene check passed ({scope}; {coverage})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
