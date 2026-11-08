"""Reject known private identifiers and high-confidence secrets.

Default mode checks tracked and nonignored untracked files. ``--staged`` checks
the actual index blobs, not their working copies. ``--history`` also
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
KEY_CHECK_MAC = "710186edca67cc8a71d72d9b22496e2300ec8f89810b945946f252216cbef530"

# HMAC-SHA256(key, casefolded identifier). Useless for recovery without the key.
FORBIDDEN_TOKEN_MACS = {
    "00d683982472ee75220d269c9b335bf5c001fc3c5e15571fe17aaed95d45e987",
    "1528aa0e824fa768b195dcc734cd164faa3c32456de27c4dcb825a4878096a84",
    "19b13b141b9edc9832efc1e0912dad383367227e7fb33f744dfc9d54bc682ba3",
    "1ebf8127ef4680d2f27ca80c0fde7121bc41e6051d68f86f6bccbc029c19c644",
    "2101e814485d3b3c1dbb4f4601391b42757a26c1d6cb7951ee3d1e777b10988f",
    "3ab2d2a1a490114e2339dbe38b6b3704cf49bae8c8553ab0bd2300618b8a623f",
    "3bb46605b859593c14f6c1c37fb7ad3db32e4591eef1def69d4130021faa7870",
    "5420cec1ed3ef3b6e6f37a79cb0e09247e8f1cc4386c6f7367eb2b672581e998",
    "589e5144d0ed96a2dedbe31dc51b2587686f21f5a1a9dac9a6a1643acc39090e",
    "62150a034a779f2c70cc76dda251f7b04cbae96cdbb32fde70eae3c92206974a",
    "6c440357dffda95efeeeee37d82b343f4198f22198c98b7e9d277985c43959f5",
    "79fa4688e897f2e81ecb78bf206d72e49b395cb2ef2ac984ca758d1c72744ea8",
    "865e5ab005880c48766220707df4f29418a91aeb827ec54d8f4aa4824222729f",
    "8b744091617a4c046e948facc19d54546873160264c72d2aeb004b9475e36cc9",
    "90281e39ac25d5a5677a713bab2b424a0c595624d646477205c4e079b9c76ef7",
    "91eefd5f4a271f59d8604d6fcd1ef04a27d428fea1434935acc47dbda1c2c715",
    "b4e33dda23edc7ee446c8edbc0bf2075dc6fcaff1e85cdd76882198a747ee7d2",
    "b9da3bc8cd7e3c097bff96362199b5cf8c43befd9801e7b10ea4aebe2ba51b32",
    "be5bf96ef518305eb8f982f9aecb2fb5cd221f91593f958c6949a04348d5a6be",
    "c35c70fcf730603961909c4887823a7876c9250e4e7dd7d80e0e41785dff5d67",
    "ca1d7a07ce1024a686ba27bd52495507f648c306999d0ca40368607a3f9145fc",
    "d55ca91acf87c0505d7b9165d00193c95f43b84abefe988e42971335c1266cb5",
    "d93153c8003b57d92966c71be519341282fcb1f0b23dad07db0e9fac4900ca48",
    "da82f67d16b70bee1848a9700366c17c86c7717ea9e243dd82cc88d23b79835e",
    "e494bbf0186952d348d31357324db8b81fa95009ccc45cc30952600dbc091942",
    "e5714b410246a2583179812f3f4256a04ad55b1ad27a554de84892ffa91c9d2e",
    "efd97f27326eca900b97ac246db8e06d62d02cd4505d215b243955e4ecedc031",
    "f2bd9d1d446ba94b77e8e041119078d0ae9a2df58d25faaa081f458a969b2dff",
    "f34c3acbc20cccb8b236d7d2b2ee8ddc31759f51fe2157a76cc31f5868057b93",
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


def _check_text(path: str, text: str) -> list[str]:
    findings: list[str] = []
    private_path = _has_forbidden(path)
    label = "<redacted path>" if private_path else path
    if private_path:
        findings.append(f"{label}: forbidden path")
    for line_number, line in enumerate(text.splitlines(), 1):
        if _has_forbidden(line):
            findings.append(f"{label}:{line_number}: forbidden private identifier")
    for name, pattern in SECRET_PATTERNS.items():
        if pattern.search(text):
            findings.append(f"{label}: possible {name}")
    return findings


def check_tracked_files(*, include_untracked: bool = False) -> list[str]:
    findings: list[str] = []
    args = ["git", "ls-files", "--cached", "-z"]
    if include_untracked:
        args.extend(["--others", "--exclude-standard"])
    for value in dict.fromkeys(subprocess.check_output(args).split(b"\0")):
        if not value:
            continue
        path = Path(value.decode())
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            findings.extend(_check_text(path.as_posix(), ""))
            continue
        findings.extend(_check_text(path.as_posix(), text))
    return findings


def check_index() -> list[str]:
    """Inspect every index entry, including staged additions hidden by clean copies."""
    findings = []
    for entry in subprocess.check_output(["git", "ls-files", "--stage", "-z"]).split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        mode, oid, stage = metadata.decode("ascii").split()
        if stage != "0":
            findings.append("Unmerged index entry; resolve before committing.")
            continue
        if mode == "160000":
            findings.extend(_check_text(raw_path.decode(), ""))
            continue
        blob = subprocess.check_output(["git", "cat-file", "blob", oid])
        findings.extend(_check_text(raw_path.decode(), blob.decode("utf-8", errors="replace")))
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
                path = "<redacted path>"
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
    parser.add_argument("--staged", action="store_true", help="check actual index blobs before commit")
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
    findings = check_index() if args.staged else check_tracked_files(include_untracked=True)
    if args.history:
        if _git("rev-parse", "--is-shallow-repository").strip() == "true":
            print("Repository hygiene check failed:\n- --history requires a full clone (fetch-depth: 0)")
            return 1
        findings.extend(check_history())

    if findings:
        print("Repository hygiene check failed:")
        print("\n".join(f"- {finding}" for finding in findings))
        return 1
    scope = "index" if args.staged else "working tree (tracked and nonignored untracked files)"
    if args.history:
        scope += " and full history"
    coverage = "identifiers and secrets" if _KEY is not None else "secrets only"
    print(f"Repository hygiene check passed ({scope}; {coverage})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
