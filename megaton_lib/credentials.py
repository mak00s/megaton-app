"""Credential discovery helpers for Google service accounts and Adobe OAuth."""

from __future__ import annotations

import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any


DEFAULT_CREDS_DIR = Path("credentials")
DEFAULT_ENV_VAR = "MEGATON_CREDS_PATH"
DEFAULT_ADOBE_ENV_VAR = "ADOBE_CREDS_PATH"


def _find_dir_upwards(dir_name: str, *, start: Path | None = None) -> Path | None:
    """Find a directory named ``dir_name`` by walking up from CWD (or ``start``)."""
    d = (start or Path.cwd()).resolve()
    while True:
        candidate = d / dir_name
        if candidate.exists() and candidate.is_dir():
            return candidate
        if d == d.parent:
            return None
        d = d.parent


def _resolve_default_dir(default_dir: Path | str) -> Path:
    """Resolve default credentials directory."""
    directory = Path(default_dir)
    if directory.is_absolute():
        return directory
    if directory.exists():
        return directory
    if directory == DEFAULT_CREDS_DIR:
        found = _find_dir_upwards(directory.name)
        if found is not None:
            return found
        pkg_creds = Path(__file__).resolve().parent.parent / directory.name
        if pkg_creds.exists() and pkg_creds.is_dir():
            return pkg_creds
    return directory


def _load_json_if_possible(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _is_service_account_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("type", "")).strip() == "service_account":
        return True
    return bool(
        str(payload.get("client_email", "")).strip()
        and str(payload.get("private_key", "")).strip()
    )


def _normalize_adobe_org_id(payload: dict[str, Any]) -> str:
    for key in ("org_id", "ims_org_id", "imsOrgId"):
        value = str(payload.get(key, "")).strip()
        if value:
            return value
    return ""


def _is_adobe_oauth_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    return bool(
        str(payload.get("client_id", "")).strip()
        and str(payload.get("client_secret", "")).strip()
        and _normalize_adobe_org_id(payload)
    )


def _list_matching_json_paths(
    *,
    env_var: str,
    default_dir: Path | str,
    predicate,
) -> list[str]:
    env_value = os.getenv(env_var, "").strip()
    if env_value.startswith(STORE_PREFIX):
        path = Path(store_service_account_path(env_value[len(STORE_PREFIX):]))
        return [str(path)] if predicate(_load_json_if_possible(path)) else []
    if env_value:
        path = Path(env_value).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"{env_var} points to missing path: {path}")
        if path.is_file():
            payload = _load_json_if_possible(path)
            return [str(path)] if predicate(payload) else []
        files = sorted(p for p in path.glob("*.json") if p.is_file())
        return [str(p) for p in files if predicate(_load_json_if_possible(p))]

    directory = _resolve_default_dir(default_dir)
    if not directory.exists():
        return []
    files = sorted(p for p in directory.glob("*.json") if p.is_file())
    return [str(p) for p in files if predicate(_load_json_if_possible(p))]


def credential_store_sa_dir() -> Path | None:
    """The megaton credential store's service-account directory, when available.

    Requires megaton>=2.6.0 (``megaton.auth.store``); ``MEGATON_HOME`` is honored.
    """
    try:
        from megaton.auth.store import CredentialStore
    except ImportError:
        return None
    try:
        directory = CredentialStore().sa_dir
    except Exception:  # noqa: BLE001 - a missing/odd store simply means "no store"
        return None
    return directory if directory.is_dir() else None


_store_tip_shown = False


def _hints_enabled() -> bool:
    try:
        from megaton.auth.hints import hints_enabled
    except ImportError:
        off = os.environ.get("MEGATON_HINTS", "").strip().lower() in {"0", "false", "no", "off"}
        ci = os.environ.get("CI", "").strip().lower() in {"1", "true", "yes"} or os.environ.get("GITHUB_ACTIONS")
        return not off and not ci
    return hints_enabled()


def _tip_repo_keys(paths: list[str], directory: Path) -> None:
    """Once per process: point repo-local keys at the machine-wide store (stderr only)."""
    global _store_tip_shown
    if _store_tip_shown or not paths or not _hints_enabled():
        return
    try:
        from megaton.auth.store import CredentialStore

        stored = {entry.client_email for entry in CredentialStore().service_accounts()}
    except Exception:  # noqa: BLE001 - core without a store: nothing to suggest
        return
    _store_tip_shown = True
    emails = {path: str((_load_json_if_possible(Path(path)) or {}).get("client_email", "")) for path in paths}
    missing = [path for path, email in emails.items() if email not in stored]
    if missing:
        first = Path(missing[0])
        message = (f"{len(missing)} service-account key(s) under {directory} are not in the megaton credential "
                   f"store. Add each once with `megaton auth import --sa {shlex.quote(str(first))} --name <name>`; when no "
                   "credentials/ directory is present the app reads the store automatically.")
    else:
        message = (f"every service-account key under {directory} is already in the megaton credential store; "
                   "the app reads the store automatically when no credentials/ directory is present.")
    print(f"[megaton-app] hint: {message} (silence with MEGATON_HINTS=0)", file=sys.stderr)


def resolve_service_account_path(
    env_var: str = DEFAULT_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> str:
    """Resolve one Google service account JSON path.

    Resolution order:
    1) If env_var is set:
       - file path: must be a service-account JSON
       - directory path: must contain exactly one service-account JSON
    2) Fallback to default_dir, which must contain exactly one service-account JSON
    """
    env_value = os.getenv(env_var, "").strip()
    if env_value.startswith(STORE_PREFIX):
        return store_service_account_path(env_value[len(STORE_PREFIX):])
    if env_value:
        return _resolve_single_json(
            Path(env_value).expanduser(),
            env_var,
            predicate=_is_service_account_payload,
            kind_label="service account JSON",
        )

    return _resolve_single_json(
        _resolve_default_dir(default_dir),
        f"{env_var} or default credentials directory",
        predicate=_is_service_account_payload,
        kind_label="service account JSON",
    )


def list_service_account_paths(
    env_var: str = DEFAULT_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> list[str]:
    """Return all valid Google service-account JSON file paths.

    Resolution order: ``env_var`` (file or directory); otherwise the default
    ``credentials/`` directory. Only when no such directory exists at all (and
    the default directory is in use) does discovery fall back to the megaton
    credential store (``~/.config/megaton/sa`` or ``$MEGATON_HOME/sa``), so a
    machine set up with ``megaton auth import --sa`` needs no per-repository
    key copies, while an existing credentials/ directory keeps full control.
    """
    paths = _list_matching_json_paths(
        env_var=env_var,
        default_dir=default_dir,
        predicate=_is_service_account_payload,
    )
    if os.getenv(env_var, "").strip() or Path(default_dir) != DEFAULT_CREDS_DIR:
        return paths
    directory = _resolve_default_dir(default_dir)
    if directory.exists():
        _tip_repo_keys(paths, directory)
        return paths
    store_dir = credential_store_sa_dir()
    if store_dir is None:
        return []
    files = sorted(p for p in store_dir.glob("*.json") if p.is_file())
    return [str(p) for p in files if _is_service_account_payload(_load_json_if_possible(p))]


def list_adobe_oauth_paths(
    env_var: str = DEFAULT_ADOBE_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> list[str]:
    """Return all valid Adobe OAuth JSON file paths."""
    return _list_matching_json_paths(
        env_var=env_var,
        default_dir=default_dir,
        predicate=_is_adobe_oauth_payload,
    )


def load_adobe_oauth_credentials(path: str | Path) -> dict[str, str]:
    """Load one Adobe OAuth JSON file with normalized keys."""
    raw_path = Path(path).expanduser()
    if not raw_path.exists():
        raise FileNotFoundError(f"Adobe OAuth credentials file not found: {raw_path}")

    payload = _load_json_if_possible(raw_path)
    if not _is_adobe_oauth_payload(payload):
        raise ValueError(f"Invalid Adobe OAuth credentials file: {raw_path}")

    assert payload is not None
    scopes = str(payload.get("scopes") or payload.get("scope") or "").strip()
    return {
        "client_id": str(payload.get("client_id", "")).strip(),
        "client_secret": str(payload.get("client_secret", "")).strip(),
        "org_id": _normalize_adobe_org_id(payload),
        "scopes": scopes,
        "source_path": str(raw_path),
    }


def _resolve_single_json(
    path: Path,
    source_label: str,
    *,
    predicate,
    kind_label: str,
) -> str:
    if path.is_file():
        payload = _load_json_if_possible(path)
        if predicate(payload):
            return str(path)
        raise RuntimeError(f"{source_label} is not a valid {kind_label}: {path}")

    if not path.exists():
        raise FileNotFoundError(f"Credentials directory not found: {path}")

    files = sorted(
        p for p in path.glob("*.json")
        if p.is_file() and predicate(_load_json_if_possible(p))
    )
    if len(files) == 1:
        return str(files[0])
    if not files:
        raise FileNotFoundError(
            f"No {kind_label} found in {path}. "
            f"Set {source_label} to a JSON file path.",
        )
    raise RuntimeError(
        f"Multiple {kind_label}s found in {path}: "
        f"{', '.join(str(p.name) for p in files)}. "
        f"Set {source_label} to the target JSON file path.",
    )


STORE_PREFIX = "store:"


def _store():
    """The machine-wide credential store (megaton >= 2.6.0, ``~/.config/megaton``)."""
    try:
        from megaton.auth.store import CredentialStore
    except ImportError as exc:
        raise RuntimeError(
            "--account / store:<name> needs the megaton credential store (megaton>=2.6.0): pip install -U megaton"
        ) from exc
    return CredentialStore()


def store_user_token(account: str) -> tuple[str, str]:
    """Resolve a store user (alias or email) to ``(token_path, email)``.

    The token file is the store's own ``users/<email>.json``; callers keep verifying
    the OAuth identity against the returned email exactly as with an explicit file.
    """
    store = _store()
    email = store.resolve_email(account)
    path = store.user_token_path(email)
    if not path.is_file():
        raise FileNotFoundError(
            f"No stored token for {email}: run `megaton auth login --account {email} --scopes ...`"
        )
    return str(path), email


def store_service_account_path(name: str) -> str:
    """Resolve a store service-account name/alias to its key file path."""
    return str(_store().service_account_path(name))


def resolve_account_args(account: str | None, token: str | None, expected_email: str | None,
                         *, token_label: str = "--token") -> tuple[str, str]:
    """Combine ``--account`` with explicit ``--token/--expected-email`` for CLIs.

    ``--account`` fills both from the store; explicit values still win so existing
    invocations are unchanged. Missing pieces raise with the exact option to pass.
    """
    if account:
        store_token, store_email = store_user_token(account)
        token = token or store_token
        expected_email = expected_email or store_email
    if not token or not expected_email:
        raise ValueError(f"pass --account <store user> or both {token_label} and --expected-email")
    return token, expected_email


__all__ = [
    "DEFAULT_ADOBE_ENV_VAR",
    "DEFAULT_CREDS_DIR",
    "DEFAULT_ENV_VAR",
    "STORE_PREFIX",
    "list_adobe_oauth_paths",
    "credential_store_sa_dir",
    "list_service_account_paths",
    "load_adobe_oauth_credentials",
    "resolve_account_args",
    "resolve_service_account_path",
    "store_service_account_path",
    "store_user_token",
]
