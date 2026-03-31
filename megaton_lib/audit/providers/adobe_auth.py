"""Shared Adobe OAuth client_credentials flow.

Provides `AdobeOAuthClient` that handles token acquisition, caching,
and auto-refresh for any Adobe API (Analytics, Target, Reactor, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import time
from typing import Any

import requests

from ..config import DEFAULT_ADOBE_SCOPES

IMS_TOKEN_URL = "https://ims-na1.adobelogin.com/ims/token/v3"

# Re-export for backward compatibility
DEFAULT_SCOPES = DEFAULT_ADOBE_SCOPES


@dataclass(slots=True)
class AdobeOAuthClient:
    """Adobe IMS OAuth client_credentials token manager.

    Handles token acquisition, file-based caching with expiry buffer,
    and transparent refresh.  Shared across AA, Reactor, and Target clients.
    """

    client_id: str = ""
    client_secret: str = ""
    org_id: str = ""
    scopes: str = DEFAULT_SCOPES
    token_cache_file: str | Path = "credentials/.adobe_token_cache.json"

    # Allow env-var indirection (resolved in __post_init__)
    client_id_env: str = "ADOBE_CLIENT_ID"
    client_secret_env: str = "ADOBE_CLIENT_SECRET"
    org_id_env: str = "ADOBE_ORG_ID"

    _token_cache_path: Path = field(init=False, repr=False)
    _access_token: str = field(init=False, repr=False)

    # ---- lifecycle --------------------------------------------------------

    def __post_init__(self) -> None:
        # Resolve credentials: explicit value > env var
        if not self.client_id:
            self.client_id = os.getenv(self.client_id_env, "").strip()
        if not self.client_id:
            raise RuntimeError(
                f"Adobe client_id is missing: set client_id or env {self.client_id_env}"
            )

        if not self.client_secret:
            self.client_secret = os.getenv(self.client_secret_env, "").strip()
        if not self.client_secret:
            raise RuntimeError(
                f"Adobe client_secret is missing: set client_secret or env {self.client_secret_env}"
            )

        if not self.org_id:
            self.org_id = os.getenv(self.org_id_env, "").strip()
        if not self.org_id:
            raise RuntimeError(
                f"Adobe org_id is missing: set org_id or env {self.org_id_env}"
            )

        self._token_cache_path = Path(self.token_cache_file)
        self._token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._access_token = self._ensure_access_token()

    # ---- public API -------------------------------------------------------

    @property
    def access_token(self) -> str:
        """Current access token (auto-acquired on init)."""
        return self._access_token

    def refresh_access_token(self) -> str:
        """Force-refresh the access token."""
        self._access_token = self._ensure_access_token(force_refresh=True)
        return self._access_token

    def get_headers(self, *, extra: dict[str, str] | None = None) -> dict[str, str]:
        """Build common Adobe API headers.

        Returns Authorization, x-api-key, x-gw-ims-org-id plus any
        caller-supplied extras (e.g. Accept, Content-Type, company header).
        """
        headers: dict[str, str] = {
            "Authorization": f"Bearer {self._access_token}",
            "x-api-key": self.client_id,
            "x-gw-ims-org-id": self.org_id,
        }
        if extra:
            headers.update(extra)
        return headers

    # ---- token management (private) ---------------------------------------

    def _ensure_access_token(self, force_refresh: bool = False) -> str:
        if not force_refresh:
            cached = self._load_cached_token()
            if cached:
                return cached
        token_info = self._request_token()
        self._save_token(token_info)
        return str(token_info["access_token"])

    def _load_cached_token(self) -> str | None:
        if not self._token_cache_path.exists():
            return None
        try:
            payload = json.loads(self._token_cache_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

        expires_at = float(payload.get("expires_at", 0))
        if expires_at - 60 <= time.time():
            return None

        token = payload.get("access_token")
        if isinstance(token, str) and token.strip():
            return token
        return None

    def _save_token(self, token_info: dict[str, Any]) -> None:
        payload = {
            "access_token": token_info.get("access_token"),
            "expires_at": time.time() + float(token_info.get("expires_in", 3600)),
        }
        self._token_cache_path.write_text(json.dumps(payload), encoding="utf-8")

    def _request_token(self) -> dict[str, Any]:
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": self.scopes,
        }
        resp = requests.post(
            IMS_TOKEN_URL,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(
                f"Adobe token request failed: HTTP {resp.status_code} {resp.text}"
            )
        try:
            payload = resp.json()
        except Exception as exc:
            raise RuntimeError("Adobe token response is not JSON") from exc

        if not isinstance(payload, dict) or "access_token" not in payload:
            raise RuntimeError("Invalid Adobe token response")
        return payload
