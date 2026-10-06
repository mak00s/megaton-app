"""Create a private Box artifact OAuth token file; does not upload files.

Configure a SEPARATE User OAuth app (read/write content only) with callback
http://localhost:8766/callback. Keep the read-only Analytics Ops app unchanged.
Client config JSON (0600): {"client_id": "...", "client_secret": "..."}.
Run: python scripts/authorize_box_artifacts.py --client-config /private/client.json
  --token-file /private/box-artifacts.json --expected-login user@example.com
Open the printed consent URL yourself and approve only the dedicated app.
"""

from __future__ import annotations

import argparse
import json
import secrets
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import requests

from megaton_lib.box_api import BoxAPIError, BoxArtifactClient, _save_private


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-config", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--expected-login", required=True)
    args = parser.parse_args(argv)
    config_path = args.client_config.expanduser().resolve()
    token_path = args.token_file.expanduser().resolve()
    if config_path.stat().st_mode & 0o077:
        raise BoxAPIError("client_config_permissions_require_0600")
    if token_path.exists():
        raise BoxAPIError("token_file_exists_select_new_path")
    config = json.loads(config_path.read_text())
    state = secrets.token_urlsafe(32)
    redirect_uri = "http://localhost:8766/callback"
    captured = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # callback query contains an authorization code

        def do_GET(self):
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            if parsed.path != "/callback" or not secrets.compare_digest(
                query.get("state", [""])[0], state
            ):
                self.send_error(400, "Invalid callback")
                return
            if captured:
                self.send_error(400, "Callback already received")
                return
            captured.update(
                code=query.get("code", [""])[0], error=query.get("error", [""])[0]
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(
                b"Consent received. Check the terminal for final verification."
            )

    with HTTPServer(("127.0.0.1", 8766), Handler) as server:
        server.timeout = 1
        print("Dedicated artifact OAuth consent URL:")
        print(
            "https://account.box.com/api/oauth2/authorize?"
            + urlencode(
                dict(
                    response_type="code",
                    client_id=config["client_id"],
                    state=state,
                    redirect_uri=redirect_uri,
                )
            )
        )
        deadline = time.monotonic() + 600
        while not captured and time.monotonic() < deadline:
            server.handle_request()
    if not captured.get("code") or captured.get("error"):
        raise BoxAPIError("oauth_consent_not_completed")
    try:
        response = requests.post(
            "https://api.box.com/oauth2/token",
            data=dict(
                grant_type="authorization_code",
                code=captured["code"],
                redirect_uri=redirect_uri,
                client_id=config["client_id"],
                client_secret=config["client_secret"],
            ),
            allow_redirects=False,
            timeout=(10, 60),
        )
        if response.status_code != 200:
            raise BoxAPIError("oauth_exchange_failed")
        tokens = response.json()
        # Persist rotated credentials before any additional network call. An actor
        # failure leaves an unverified file which must not be used by consumers.
        data = {
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "expires_at": time.time() + int(tokens["expires_in"]),
            "actor_verified": False,
        }
        _save_private(token_path, data)
        BoxArtifactClient(tokens["access_token"], expected_login=args.expected_login)
        data["actor_verified"] = True
        data["expected_login"] = args.expected_login
        _save_private(token_path, data)
    except (requests.RequestException, ValueError, KeyError):
        raise BoxAPIError("oauth_setup_failed") from None
    print("Verified actor; private token saved. No Box files were changed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BoxAPIError as exc:
        raise SystemExit(str(exc)) from None
