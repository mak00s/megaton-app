from __future__ import annotations

from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.auth.credentials import Credentials as BaseCredentials
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


def refresh_user_credentials(creds: Credentials) -> Credentials:
    """Return valid user OAuth credentials, refreshing when possible."""
    if creds.valid:
        return creds
    if creds.refresh_token:
        creds.refresh(Request())
        if creds.valid:
            return creds
    raise RuntimeError("Google user credentials are invalid and cannot be refreshed.")


def authorize_user_credentials(
    *,
    client_secrets_path: str | Path | None = None,
    token_path: str | Path,
    scopes: list[str],
    expected_email: str | None = None,
) -> Credentials:
    """Run an installed-app OAuth flow once, then reuse the saved token."""
    token = Path(token_path).expanduser()
    creds: Credentials | None = None
    if token.exists():
        creds = Credentials.from_authorized_user_file(str(token), scopes)

    if creds:
        try:
            return refresh_user_credentials(creds)
        except RuntimeError:
            pass

    if not client_secrets_path:
        raise RuntimeError(
            "Google OAuth client secrets are required when the token file is missing or cannot refresh."
        )

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "google-auth-oauthlib is required for interactive Google OAuth."
        ) from exc

    flow = InstalledAppFlow.from_client_secrets_file(
        str(Path(client_secrets_path).expanduser().resolve()),
        scopes,
    )
    creds = flow.run_local_server(
        port=0,
        open_browser=True,
        authorization_prompt_message=(
            f"ブラウザで {expected_email or '対象 Google アカウント'} にログインして認可してください..."
        ),
        success_message="認可完了。ブラウザを閉じて OK です。",
    )
    token.parent.mkdir(parents=True, exist_ok=True)
    token.write_text(creds.to_json(), encoding="utf-8")
    token.chmod(0o600)
    return creds


def build_user_service(
    api_name: str,
    version: str,
    *,
    client_secrets_path: str | Path | None = None,
    token_path: str | Path,
    scopes: list[str],
    expected_email: str | None = None,
    **build_kwargs: Any,
):
    creds = authorize_user_credentials(
        client_secrets_path=client_secrets_path,
        token_path=token_path,
        scopes=scopes,
        expected_email=expected_email,
    )
    return build(api_name, version, credentials=creds, cache_discovery=False, **build_kwargs)


def build_service(
    api_name: str,
    version: str,
    *,
    credentials: BaseCredentials,
    **build_kwargs: Any,
):
    """Build a Google API service from already prepared credentials.

    Use this when callers need custom credential loading or validation before
    service construction, while still keeping googleapiclient creation behind
    the shared workspace helper.
    """
    return build(api_name, version, credentials=credentials, cache_discovery=False, **build_kwargs)


def build_service_account_credentials(
    credentials_path: str | Path,
    *,
    scopes: list[str],
) -> service_account.Credentials:
    return service_account.Credentials.from_service_account_file(
        str(Path(credentials_path).expanduser().resolve()),
        scopes=scopes,
    )


def build_service_account_service(
    api_name: str,
    version: str,
    *,
    credentials_path: str | Path,
    scopes: list[str],
    **build_kwargs: Any,
):
    creds = build_service_account_credentials(credentials_path, scopes=scopes)
    return build(api_name, version, credentials=creds, cache_discovery=False, **build_kwargs)
