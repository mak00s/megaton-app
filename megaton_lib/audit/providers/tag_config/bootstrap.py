"""Bootstrap helpers for Adobe Tags config setup."""

from __future__ import annotations

import configparser
from collections.abc import Callable, Sequence
import os
from pathlib import Path
import tomllib
from typing import Any, Mapping

from ...config import AdobeOAuthConfig, AdobeTagsConfig, DEFAULT_ADOBE_SCOPES

DEFAULT_ANALYSIS_ACCOUNTS = ("csk", "wws", "dms")


def load_env_file(path: str | Path, *, override: bool = False) -> None:
    """Load simple ``KEY=VALUE`` pairs into ``os.environ`` if present."""
    env_path = Path(path).expanduser().resolve()
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        resolved_key = key.strip()
        resolved_value = value.strip().strip('"').strip("'")
        if override:
            os.environ[resolved_key] = resolved_value
        else:
            os.environ.setdefault(resolved_key, resolved_value)


def account_token_cache_file(
    account: str = "",
    *,
    project_root: str | Path = ".",
    token_cache_dir: str | Path = "credentials",
    filename_template: str = ".adobe_token_cache.{account}.json",
    account_env_var: str = "ACCOUNT",
    fallback_account: str = "default",
) -> Path:
    """Return an account-namespaced Adobe OAuth token cache path.

    ``token_cache_dir`` may be absolute or relative to ``project_root``.
    """
    root = Path(project_root).expanduser().resolve()
    cache_dir = Path(token_cache_dir).expanduser()
    if not cache_dir.is_absolute():
        cache_dir = root / cache_dir

    resolved_account = (
        account.strip()
        or os.getenv(account_env_var, "").strip()
        or fallback_account.strip()
        or "default"
    )
    safe_account = "".join(
        char if char.isalnum() or char in ("-", "_", ".") else "_"
        for char in resolved_account
    )
    filename = filename_template.format(account=safe_account)
    return (cache_dir / filename).resolve()


def _resolve_candidate_path(project_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else project_root / path


def resolve_first_existing_path(
    explicit: str | Path = "",
    *,
    project_root: str | Path = ".",
    candidates: Sequence[str | Path] = (),
) -> Path | None:
    """Resolve the first existing path from an explicit value or candidates."""
    root = Path(project_root).expanduser().resolve()
    if str(explicit).strip():
        return _resolve_candidate_path(root, explicit)
    for candidate in candidates:
        path = _resolve_candidate_path(root, candidate)
        if path.exists():
            return path
    return None


def _default_account_from_pyproject(project_root: Path) -> str:
    path = project_root / "pyproject.toml"
    if not path.exists():
        return ""
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError:
        return ""

    tool = data.get("tool", {})
    if not isinstance(tool, dict):
        return ""
    for table_name in ("megaton", "tags"):
        table = tool.get(table_name, {})
        if isinstance(table, dict):
            account = str(table.get("default_account", "")).strip()
            if account:
                return account
    return ""


def _single_env_account(project_root: Path, known_accounts: tuple[str, ...]) -> str:
    found: list[str] = []
    for path in sorted(project_root.glob(".env.*")):
        account = path.name.removeprefix(".env.")
        if not known_accounts or account in known_accounts:
            found.append(account)
    return found[0] if len(found) == 1 else ""


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)]


def _git_origin_url(project_root: Path) -> str:
    for path in (project_root, *project_root.parents):
        git_config = path / ".git" / "config"
        if not git_config.exists():
            continue
        parser = configparser.ConfigParser()
        parser.read(git_config, encoding="utf-8")
        section = 'remote "origin"'
        if parser.has_option(section, "url"):
            return parser.get(section, "url").strip()
    return ""


def _account_from_hints(
    *,
    project_root: Path,
    known_accounts: tuple[str, ...],
    account_hints: Mapping[str, Mapping[str, Any]] | None,
    property_id: str,
    library_id: str,
    git_remote_url: str,
) -> str:
    if not account_hints:
        return ""

    remote_url = (git_remote_url or _git_origin_url(project_root)).lower()
    root_text = str(project_root).lower()
    property_value = property_id.strip()
    library_value = library_id.strip()
    matches: list[str] = []

    for account, raw_hints in account_hints.items():
        resolved_account = str(account).strip()
        if not resolved_account or (known_accounts and resolved_account not in known_accounts):
            continue
        hints = dict(raw_hints or {})
        matched = False

        property_ids = {item.strip() for item in _as_list(hints.get("property_ids")) if item.strip()}
        if property_value and property_value in property_ids:
            matched = True

        library_ids = {item.strip() for item in _as_list(hints.get("library_ids")) if item.strip()}
        if library_value and library_value in library_ids:
            matched = True

        for needle in _as_list(hints.get("remote_contains")):
            if needle.strip().lower() and needle.strip().lower() in remote_url:
                matched = True
                break

        for needle in _as_list(hints.get("path_contains")) + _as_list(hints.get("cwd_contains")):
            if needle.strip().lower() and needle.strip().lower() in root_text:
                matched = True
                break

        if matched:
            matches.append(resolved_account)

    unique_matches = sorted(set(matches))
    if len(unique_matches) > 1:
        raise RuntimeError(f"Account hints are ambiguous: {', '.join(unique_matches)}")
    return unique_matches[0] if unique_matches else ""


def bootstrap_account_env(
    account: str = "",
    *,
    project_root: str | Path = ".",
    known_accounts: tuple[str, ...] = DEFAULT_ANALYSIS_ACCOUNTS,
    account_env_var: str = "ACCOUNT",
    load_base_env: bool = True,
    account_hints: Mapping[str, Mapping[str, Any]] | None = None,
    property_id: str = "",
    library_id: str = "",
    git_remote_url: str = "",
) -> str:
    """Resolve an analysis account and load its repo-local env file.

    Resolution order:
    1. explicit ``account`` argument
    2. ``ACCOUNT`` environment variable
    3. ``[tool.megaton].default_account`` or ``[tool.tags].default_account``
    4. optional ``account_hints`` matched against property/library/remote/path values
    5. the only matching ``.env.<account>`` file under ``project_root``
    """
    root = Path(project_root).expanduser().resolve()
    explicit_account = account.strip()
    resolved = (
        explicit_account
        or os.getenv(account_env_var, "").strip()
        or _default_account_from_pyproject(root)
        or _account_from_hints(
            project_root=root,
            known_accounts=known_accounts,
            account_hints=account_hints,
            property_id=property_id,
            library_id=library_id,
            git_remote_url=git_remote_url,
        )
        or _single_env_account(root, known_accounts)
    )

    if not resolved:
        raise RuntimeError(
            f"{account_env_var} is required. Pass --account, set {account_env_var}, "
            "define [tool.megaton].default_account in pyproject.toml, "
            "or provide account_hints.",
        )
    if known_accounts and resolved not in known_accounts:
        raise RuntimeError(
            f"Unknown {account_env_var}={resolved}. "
            f"Expected one of: {', '.join(sorted(known_accounts))}",
        )

    env_path = root / f".env.{resolved}"
    if not env_path.exists():
        raise RuntimeError(f"Missing env file: {env_path}")

    load_env_file(env_path, override=bool(explicit_account))
    if load_base_env:
        load_env_file(root / ".env")
    if explicit_account:
        os.environ[account_env_var] = resolved
    else:
        os.environ.setdefault(account_env_var, resolved)
    return resolved


def merge_adobe_scopes(
    base_scopes: str | None,
    *,
    required: tuple[str, ...] = ("read_organizations", "additional_info.roles"),
) -> str:
    """Return a comma-separated scope string with required scopes appended."""
    source = (base_scopes or DEFAULT_ADOBE_SCOPES).strip()
    ordered = [item.strip() for item in source.split(",") if item.strip()]
    seen = set(ordered)
    for item in required:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ",".join(ordered)


def seed_adobe_oauth_env(
    *,
    payload: Mapping[str, Any] | None = None,
    creds_file: str | Path | None = None,
    client_id: str = "",
    client_secret: str = "",
    org_id: str = "",
    client_id_env: str = "ADOBE_CLIENT_ID",
    client_secret_env: str = "ADOBE_CLIENT_SECRET",
    org_id_env: str = "ADOBE_ORG_ID",
) -> tuple[str, str, str]:
    """Resolve Adobe OAuth values from explicit args, env, JSON file, and optional payload.

    Resolution order (first non-empty wins):
    1. Explicit function arguments (``client_id``, ``client_secret``, ``org_id``)
    2. Environment variables (``ADOBE_CLIENT_ID``, etc.)
    3. JSON credential file (``creds_file``) — loaded via ``load_adobe_oauth_credentials``
    4. ``payload`` dict
    """
    merged_payload: dict[str, Any] = dict(payload or {})

    if creds_file is not None:
        creds_path = Path(creds_file).expanduser().resolve()
        if creds_path.exists():
            from ....credentials import load_adobe_oauth_credentials  # noqa: WPS433

            file_creds = load_adobe_oauth_credentials(creds_path)
            for key in ("client_id", "client_secret", "org_id"):
                if file_creds.get(key) and not merged_payload.get(key):
                    merged_payload[key] = file_creds[key]

    payload = merged_payload

    resolved_client_id = (
        client_id.strip()
        or os.getenv(client_id_env, "").strip()
        or str(payload.get("client_id", "")).strip()
    )
    resolved_client_secret = (
        client_secret.strip()
        or os.getenv(client_secret_env, "").strip()
        or str(payload.get("client_secret", "")).strip()
    )
    resolved_org_id = (
        org_id.strip()
        or os.getenv(org_id_env, "").strip()
        or str(payload.get("org_id", "")).strip()
    )

    if not resolved_client_id:
        raise RuntimeError(
            f"Adobe client_id is missing: set {client_id_env} env var, "
            "pass client_id explicitly, or provide a creds_file"
        )
    if not resolved_client_secret:
        raise RuntimeError(
            f"Adobe client_secret is missing: set {client_secret_env} env var, "
            "pass client_secret explicitly, or provide a creds_file"
        )

    os.environ[client_id_env] = resolved_client_id
    os.environ[client_secret_env] = resolved_client_secret
    if resolved_org_id:
        os.environ[org_id_env] = resolved_org_id

    return resolved_client_id, resolved_client_secret, resolved_org_id


def adobe_tags_output_root(property_id: str = "") -> Path:
    """Return the canonical export root for one Adobe Tags property.

    Resolution order (all evaluated at call time via ``os.getenv``):
      1. ``ANALYSIS_ADOBE_TAGS_OUTPUT_ROOT`` (explicit override)
      2. ``TAGS_OUTPUT_ROOT`` (``.env`` shorthand)
      3. ``adobe-tags/{property_id}`` (fallback)
    """
    override = os.getenv("ANALYSIS_ADOBE_TAGS_OUTPUT_ROOT", "").strip()
    if not override:
        override = os.getenv("TAGS_OUTPUT_ROOT", "").strip()
    if override:
        return Path(override)

    resolved = property_id.strip()
    if not resolved:
        raise RuntimeError("Adobe Tags property_id is required to resolve export root")
    return Path("adobe-tags") / resolved


def build_tags_config(
    *,
    property_id: str,
    page_size: int = 100,
    scopes: str | None = None,
    token_cache_file: str | Path = "credentials/.adobe_token_cache.json",
    payload: Mapping[str, Any] | None = None,
    creds_file: str | Path | None = None,
    client_id: str = "",
    client_secret: str = "",
    org_id: str = "",
    client_id_env: str = "ADOBE_CLIENT_ID",
    client_secret_env: str = "ADOBE_CLIENT_SECRET",
    org_id_env: str = "ADOBE_ORG_ID",
) -> AdobeTagsConfig:
    """Build ``AdobeTagsConfig`` after resolving OAuth settings."""
    if not property_id.strip():
        raise RuntimeError("Adobe Tags property_id is required")

    payload = payload or {}
    _, _, resolved_org_id = seed_adobe_oauth_env(
        payload=payload,
        creds_file=creds_file,
        client_id=client_id,
        client_secret=client_secret,
        org_id=org_id,
        client_id_env=client_id_env,
        client_secret_env=client_secret_env,
        org_id_env=org_id_env,
    )

    cache_path = Path(token_cache_file).expanduser().resolve()
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    oauth = AdobeOAuthConfig(
        client_id_env=client_id_env,
        client_secret_env=client_secret_env,
        org_id_env=org_id_env,
        org_id=resolved_org_id or None,
        scopes=merge_adobe_scopes(scopes or str(payload.get("scopes", "")).strip()),
        token_cache_file=str(cache_path),
    )

    return AdobeTagsConfig(
        property_id=property_id.strip(),
        oauth=oauth,
        page_size=page_size,
    )


def build_repo_tags_config_factory(
    *,
    project_root: str | Path = ".",
    credentials_candidates: Sequence[str | Path] = (),
    token_cache_dir: str | Path = "credentials",
    account_default: str = "",
    org_id: str = "",
    adobe_config_env: str = "ANALYSIS_ADOBE_CONFIG_PATH",
    env_file_var: str = "ENV_FILE",
    default_env_file: str = ".env",
    account_env_var: str = "ACCOUNT",
    token_cache_filename_template: str = ".adobe_token_cache.{account}.json",
    client_id_env: str = "ADOBE_CLIENT_ID",
    client_secret_env: str = "ADOBE_CLIENT_SECRET",
    org_id_env: str = "ADOBE_ORG_ID",
) -> Callable[..., AdobeTagsConfig]:
    """Build a repo-local ``AdobeTagsConfig`` factory for analysis wrappers.

    The returned callable accepts ``property_id`` and ``page_size`` keyword args.
    It loads ``ENV_FILE`` if present, resolves the first existing credentials
    file from ``credentials_candidates``, and stores OAuth tokens in an
    account-namespaced cache.
    """
    root = Path(project_root).expanduser().resolve()
    candidates = tuple(credentials_candidates)

    def factory(*, property_id: str, page_size: int = 100) -> AdobeTagsConfig:
        env_file = os.getenv(env_file_var, default_env_file).strip()
        if env_file:
            load_env_file(root / env_file)

        creds_file = resolve_first_existing_path(
            os.getenv(adobe_config_env, "").strip(),
            project_root=root,
            candidates=candidates,
        )
        token_cache = account_token_cache_file(
            os.getenv(account_env_var, "").strip() or account_default,
            project_root=root,
            token_cache_dir=token_cache_dir,
            filename_template=token_cache_filename_template,
            account_env_var=account_env_var,
            fallback_account=account_default or "default",
        )
        token_cache.parent.mkdir(parents=True, exist_ok=True)

        return build_tags_config(
            property_id=property_id,
            page_size=page_size,
            token_cache_file=token_cache,
            creds_file=creds_file,
            org_id=org_id,
            client_id_env=client_id_env,
            client_secret_env=client_secret_env,
            org_id_env=org_id_env,
        )

    return factory
