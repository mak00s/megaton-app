"""Explicit user authorization and read-only GTM audit snapshots."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import os

from .gtm_client import GtmClient, authorize_gtm_user


def _write_private(path: Path, payload) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".gtm-review-", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=(
        "GTM audit with dedicated user OAuth. No GTM edits or publication by this CLI. "
        "auth --access edit grants container/version editing, never publication. "
        "Only auth opens a consent browser; reads never fall back to SA/ADC."
    ))
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("auth", "Explicit initial consent; saves a private token after account verification."),
        ("containers", "List accessible containers via API; saves a private JSON inventory."),
        ("review", "Read all workspaces and live version; saves a private JSON audit snapshot."),
    ):
        command = commands.add_parser(name, help=help_text, description=help_text)
        command.add_argument("--token", required=True, type=Path, help="Dedicated GTM OAuth token JSON; never an SA key.")
        command.add_argument("--expected-email", required=True, help="Verified Google account that must match.")
        if name == "auth":
            command.add_argument("--client-secrets", required=True, type=Path, help="Desktop OAuth client JSON.")
            command.add_argument("--access", choices=("read", "edit"), default="read",
                                 help="Explicit grant: read (default), or container/version editing without publish/delete/access administration. Existing tokens are never upgraded automatically.")
        else:
            command.add_argument("--output", required=True, type=Path, help="Private local JSON output (gitignored).")
        if name == "review":
            command.add_argument("--container-path", required=True, action="append",
                                 help="accounts/ACCOUNT/containers/CONTAINER; repeat for multiple containers.")
    args = parser.parse_args(argv)
    print(f"gtm_mode={args.command} remote_writes=none auth_fallback=none", file=sys.stderr)
    if args.command == "auth":
        print(f"oauth_access={args.access} publish_allowed=false token_upgrade=none", file=sys.stderr)
    try:
        if args.command == "auth":
            authorize_gtm_user(client_secrets_path=args.client_secrets, token_path=args.token,
                               expected_email=args.expected_email, access=args.access)
            print("gtm_auth=verified token_saved_or_reused=true", file=sys.stderr)
            return 0
        if args.output.expanduser().resolve() == args.token.expanduser().resolve():
            raise ValueError("Output must not overwrite the OAuth token.")
        if args.command == "review":
            from .gtm_client import _numeric_path
            for path in args.container_path:
                _numeric_path(path)
        client = GtmClient.from_oauth_file(args.token, expected_email=args.expected_email)
        result = ({"schema_version": 1, "mode": "read_only", "containers": client.containers(),
                   "fetched_at": datetime.now(timezone.utc).isoformat()}
                  if args.command == "containers" else
                  {"schema_version": 1, "mode": "read_only",
                   "reviews": [client.review(path) for path in dict.fromkeys(args.container_path)]})
        _write_private(args.output, result)
        print(f"gtm_read=complete output={args.output}", file=sys.stderr)
        return 0
    except Exception as exc:
        # Google/OAuth exceptions can contain authorization URLs or token details.
        from .gtm_client import GtmAuthError
        status = getattr(getattr(exc, "resp", None), "status", None)
        safe_status = f" HTTP {status}" if isinstance(status, int) else ""
        message = str(exc) if isinstance(exc, GtmAuthError) else (
            f"{type(exc).__name__}{safe_status}: failed; no fallback or reauthorization. "
            "Check credentials, scopes and GTM access."
        )
        print(f"gtm_error={message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
