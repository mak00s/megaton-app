"""GTM audit access and explicitly selected, non-publishing user OAuth."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile
from datetime import datetime, timezone

from .google_workspace import build_service, refresh_user_credentials

SCOPES_READ = [
    "https://www.googleapis.com/auth/tagmanager.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
]
SCOPES_EDIT = SCOPES_READ + [
    "https://www.googleapis.com/auth/tagmanager.edit.containers",
    "https://www.googleapis.com/auth/tagmanager.edit.containerversions",
]

_WORKSPACE_RESOURCES = (
    ("tags", "tag", "supportTags"),
    ("triggers", "trigger", "supportTriggers"),
    ("variables", "variable", "supportVariables"),
    ("folders", "folder", "supportFolders"),
    ("templates", "template", "supportTemplates"),
    ("built_in_variables", "builtInVariable", "supportBuiltInVariables"),
    ("clients", "client", "supportClients"),
    ("transformations", "transformation", "supportTransformations"),
    ("zones", "zone", "supportZones"),
    ("gtag_config", "gtagConfig", "supportGtagConfigs"),
)


def _access_scopes(access: str) -> list[str]:
    if access not in {"read", "edit"}:
        raise GtmAuthError("access must be read or edit; publication is not supported.")
    return SCOPES_READ if access == "read" else SCOPES_EDIT


class GtmAuthError(ValueError):
    """Safe-to-display authentication/preflight failure."""


def _require_scopes(scopes, *, access: str = "read") -> None:
    required = _access_scopes(access)
    if isinstance(scopes, str):
        scopes = scopes.split()
    if not isinstance(scopes, (list, tuple)) or not all(isinstance(s, str) for s in scopes):
        raise GtmAuthError("Use a dedicated GTM token with recorded scopes.")
    granted = set(scopes)
    if not set(required).issubset(granted):
        raise GtmAuthError(f"Missing GTM {access}/userinfo.email scope; explicit authorization required.")
    if granted - set(SCOPES_EDIT) - {"openid"}:
        raise GtmAuthError("Use a dedicated non-publishing GTM token; other APIs, publish, deletion and access administration are forbidden.")


def _verify_identity(creds, expected_email: str) -> None:
    if not expected_email or not expected_email.strip():
        raise GtmAuthError("expected_email is required.")
    identity = build_service("oauth2", "v2", credentials=creds).userinfo().get().execute()
    if (not identity.get("verified_email")
            or identity.get("email", "").casefold() != expected_email.strip().casefold()):
        raise GtmAuthError("OAuth account does not match the expected verified email.")


def _load_credentials(token_path: str | Path, *, expected_email: str, access: str = "read"):
    from google.oauth2.credentials import Credentials

    if not expected_email or not expected_email.strip():
        raise GtmAuthError("expected_email is required.")
    payload = json.loads(Path(token_path).expanduser().read_text(encoding="utf-8"))
    if (not isinstance(payload, dict) or payload.get("type") not in (None, "authorized_user")
            or "private_key" in payload):
        raise GtmAuthError("GTM audit requires a user OAuth token, not a service account.")
    _require_scopes(payload.get("scopes"), access=access)
    # Preserve the saved grant when refreshing; scope names cannot create consent.
    creds = refresh_user_credentials(Credentials.from_authorized_user_info(payload))
    if creds.granted_scopes is not None:
        _require_scopes(creds.granted_scopes, access=access)
    _verify_identity(creds, expected_email)
    return creds


def authorize_gtm_user(*, client_secrets_path: str | Path, token_path: str | Path,
                       expected_email: str, access: str = "read") -> None:
    """Explicit initial desktop consent; verify identity before atomically saving.

    An existing token is validated/reused, never overwritten or automatically
    reauthorized. Runtime reads use GtmClient.from_oauth_file instead.
    """
    scopes = _access_scopes(access)
    if not expected_email or not expected_email.strip():
        raise GtmAuthError("expected_email is required.")
    token = Path(token_path).expanduser()
    client = Path(client_secrets_path).expanduser()
    if token.resolve() == client.resolve():
        raise GtmAuthError("Client JSON and token must use different paths.")
    if token.exists():
        _load_credentials(token, expected_email=expected_email, access=access)
        return
    payload = json.loads(client.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "installed" not in payload:
        raise GtmAuthError("Use a Desktop app OAuth client JSON, not a service-account key.")
    from google_auth_oauthlib.flow import InstalledAppFlow

    flow = InstalledAppFlow.from_client_secrets_file(str(client), scopes)
    try:
        creds = flow.run_local_server(
            port=0, open_browser=True, prompt="consent", access_type="offline",
            login_hint=expected_email,
            authorization_prompt_message="Authorize the explicitly selected GTM account in your browser.",
            success_message="Authorization received. Return to the CLI for account verification.",
        )
    except Warning as exc:
        # oauthlib rejects even harmless added openid scopes. Validate the actual
        # grant locally instead of globally relaxing token-scope verification.
        actual = getattr(exc, "new_scope", None)
        token_response = getattr(exc, "token", None)
        if not isinstance(token_response, dict) or actual is None:
            raise
        _require_scopes(actual, access=access)
        _require_scopes(token_response.get("scope"), access=access)
        flow.oauth2session.token = dict(token_response)
        creds = flow.credentials
    _require_scopes(creds.granted_scopes if creds.granted_scopes is not None else creds.scopes,
                    access=access)
    _verify_identity(creds, expected_email)
    if not creds.refresh_token:
        raise GtmAuthError("No refresh token received; token was not saved.")
    token.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=token.parent,
                                         prefix=".gtm-token-", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(creds.to_json())
        # Do not clobber a token created by another authorization process.
        os.link(temporary, token)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _numeric_path(path: str, suffix: str = "") -> str:
    pattern = r"accounts/\d+/containers/\d+" + suffix
    if not isinstance(path, str) or not re.fullmatch(pattern, path):
        raise ValueError("Use an explicit GTM API resource path with numeric IDs.")
    return path


class GtmClient:
    """Narrow read-only audit client; no edits, preview creation, sync or publish."""

    def __init__(self, service):
        self._service = service

    @classmethod
    def from_oauth_file(cls, token_path: str | Path, *, expected_email: str):
        """Read/refresh a dedicated token; no ADC, SA or interactive fallback."""
        creds = _load_credentials(token_path, expected_email=expected_email)
        return cls(build_service("tagmanager", "v2", credentials=creds))

    @staticmethod
    def _list(resource, key: str, **kwargs) -> list[dict]:
        items = []
        seen = set()
        while True:
            response = resource.list(**kwargs).execute()
            items.extend(response.get(key, []))
            page = response.get("nextPageToken")
            if not page:
                return items
            if page in seen:
                raise RuntimeError("GTM returned a repeated page token; incomplete inventory.")
            seen.add(page)
            kwargs["pageToken"] = page

    def containers(self) -> list[dict]:
        accounts = self._service.accounts()
        result = []
        for account in self._list(accounts, "account"):
            result.extend(self._list(accounts.containers(), "container", parent=account["path"]))
        return result

    def review(self, container_path: str) -> dict:
        """Snapshot all workspaces, their status, and the actually published version.

        Workspace status is relative to its base container version (not necessarily
        live). The separate live snapshot is the publication comparison baseline.
        Conflicts are recorded, never resolved. Retrieval is not an atomic snapshot.
        """
        path = _numeric_path(container_path)
        containers = self._service.accounts().containers()
        container = containers.get(path=path).execute()
        features = container.get("features")
        if not isinstance(features, dict) or any(
            not isinstance(value, bool) for value in features.values()
        ):
            raise RuntimeError("GTM container feature metadata is missing or invalid; review stopped.")
        live = containers.versions().live(parent=path).execute()
        workspaces = containers.workspaces()
        snapshots = []
        for workspace in self._list(workspaces, "workspace", parent=path):
            ws_path = _numeric_path(workspace["path"], r"/workspaces/\d+")
            resources = {}
            unsupported = []
            for plural, singular, feature in _WORKSPACE_RESOURCES:
                if not features.get(feature, False):
                    unsupported.append(plural)
                    continue
                resource = getattr(workspaces, plural)()
                resources[plural] = self._list(resource, singular, parent=ws_path)
            snapshots.append({"workspace": workspace,
                              "status": workspaces.getStatus(path=ws_path).execute(),
                              "resources": resources,
                              "unsupported_resources": unsupported})
        return {"schema_version": 1, "mode": "read_only", "container": container,
                "live_version": live, "workspaces": snapshots,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "snapshot_atomic": False}
