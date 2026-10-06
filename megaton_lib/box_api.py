"""Narrow Box REST adapter for caller-selected analytics artifacts.

No browser fallback, automatic write retries, deletion, or public-link creation.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
import time
import zipfile
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests


class BoxAPIError(RuntimeError):
    """Safe error code only; upstream bodies and credentials are never included."""


def _id(value: str) -> str:
    if not re.fullmatch(r"\d+", str(value)):
        raise BoxAPIError("invalid_box_id")
    return str(value)


def _name(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or any(c in value for c in '/\\\r\n\x00"')
    ):
        raise BoxAPIError("invalid_file_name")
    return value


def _validate_upload(path: Path, existing_policy: str) -> None:
    _name(path.name)
    if existing_policy not in {"error", "version"}:
        raise BoxAPIError("invalid_existing_policy")
    if not path.is_file() or path.stat().st_size > 50 * 1024 * 1024:
        raise BoxAPIError("upload_file_missing_or_over_50mib")


def _save_private(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(data, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class OAuthTokenFile:
    """POSIX lock + atomic rotation. Unknown refresh outcome requires new consent.

    Caller provisions a dedicated OAuth token file outside the checkout. Format:
    client_id, client_secret, access_token, refresh_token, expires_at (Unix seconds).
    """

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()

    def __call__(self) -> str:
        import fcntl

        if self.path.stat().st_mode & 0o077:
            raise BoxAPIError("token_file_permissions_require_0600")
        fd = os.open(str(self.path) + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(fd, "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            data = json.loads(self.path.read_text())
            if data.get("refresh_pending"):
                raise BoxAPIError("reauthentication_required")
            if float(data.get("expires_at", 0)) > time.time() + 120:
                return str(data["access_token"])
            refresh_fields = {
                k: data[k] for k in ("client_id", "client_secret", "refresh_token")
            }
            data["refresh_pending"] = True
            _save_private(self.path, data)
            try:
                response = requests.post(
                    "https://api.box.com/oauth2/token",
                    data={
                        "grant_type": "refresh_token",
                        **refresh_fields,
                    },
                    timeout=(10, 60),
                    allow_redirects=False,
                )
                if response.status_code != 200:
                    raise BoxAPIError("reauthentication_required")
                refreshed = response.json()
                if not refreshed.get("access_token") or not refreshed.get(
                    "refresh_token"
                ):
                    raise BoxAPIError("reauthentication_required")
                data.update(
                    access_token=refreshed["access_token"],
                    refresh_token=refreshed["refresh_token"],
                    expires_at=time.time() + int(refreshed["expires_in"]),
                    refresh_pending=False,
                )
                _save_private(self.path, data)
                return str(data["access_token"])
            except requests.ConnectTimeout:
                # requests documents ConnectTimeout as safe to retry: no HTTP
                # request reached Box. Generic ConnectionError is NOT equivalent.
                data["refresh_pending"] = False
                _save_private(self.path, data)
                raise BoxAPIError("refresh_connection_timeout") from None
            except (requests.RequestException, ValueError, KeyError):
                raise BoxAPIError("reauthentication_required") from None


class BoxArtifactClient:
    def __init__(self, token, *, expected_login: str, session=None, allow_write=False):
        if not expected_login:
            raise BoxAPIError("expected_box_login_required")
        self.token = token if callable(token) else lambda: token
        self.session = session or requests.Session()
        self.allow_write = allow_write
        self.shared_context = {}
        actor = self.request("GET", "/users/me", params={"fields": "id,login"})
        if str(actor.get("login", "")).casefold() != expected_login.casefold():
            raise BoxAPIError("box_actor_mismatch")

    def request(self, method, path, *, upload=False, **kwargs):
        if method != "GET" and not self.allow_write:
            raise BoxAPIError("write_not_enabled")
        base = "https://upload.box.com/api/2.0" if upload else "https://api.box.com/2.0"
        headers = {
            "Authorization": f"Bearer {self.token()}",
            "Accept": "application/json",
        }
        headers.update(self.shared_context)
        headers.update(kwargs.pop("headers", {}))
        try:
            response = self.session.request(
                method,
                base + path,
                headers=headers,
                timeout=(10, 120),
                allow_redirects=False,
                **kwargs,
            )
        except requests.RequestException:
            raise BoxAPIError(
                "network_error" if method == "GET" else "write_outcome_unknown"
            ) from None
        if not 200 <= response.status_code < 300:
            code = (
                "write_outcome_unknown"
                if method != "GET" and response.status_code >= 500
                else f"box_http_{response.status_code}"
            )
            raise BoxAPIError(code)
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError()
            return data
        except ValueError:
            raise BoxAPIError(
                "invalid_response" if method == "GET" else "write_outcome_unknown"
            ) from None

    def resolve(self, url: str) -> dict:
        try:
            if not isinstance(url, str) or any(ord(c) < 32 for c in url):
                raise ValueError()
            parsed = urlsplit(url)
            host = parsed.hostname or ""
            valid_host = re.fullmatch(
                r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)?(?:app\.)?box\.com", host
            )
            if (
                parsed.scheme != "https"
                or not valid_host
                or parsed.username
                or parsed.password
                or parsed.port
            ):
                raise ValueError()
        except ValueError:
            raise BoxAPIError("invalid_box_url") from None
        self.shared_context = {}
        match = re.fullmatch(r"/(file|folder)/(\d+)/?", parsed.path)
        if match:
            kind, item_id = match.groups()
            return self.request(
                "GET",
                f"/{kind}s/{item_id}",
                params={"fields": "id,type,name,sha1,size,etag,parent,shared_link"},
            )
        if not re.fullmatch(r"/s/[A-Za-z0-9]+/?", parsed.path):
            raise BoxAPIError("unsupported_box_url")
        canonical_url = urlunsplit(("https", host, parsed.path, "", ""))
        self.shared_context = {"BoxApi": f"shared_link={canonical_url}"}
        return self.request(
            "GET",
            "/shared_items",
            headers=self.shared_context,
            params={"fields": "id,type,name,sha1,size,etag,parent,shared_link"},
        )

    def children(self, folder_id: str) -> list[dict]:
        entries = []
        for offset in range(0, 2000, 100):
            result = self.request(
                "GET",
                f"/folders/{_id(folder_id)}/items",
                params={
                    "limit": 100,
                    "offset": offset,
                    "fields": "id,type,name,sha1,size,etag,parent",
                },
            )
            page = result.get("entries")
            if not isinstance(page, list) or len(page) > 100:
                raise BoxAPIError("invalid_response")
            entries.extend(page)
            if offset + len(page) >= int(result["total_count"]):
                return entries
            if not page:
                raise BoxAPIError("invalid_response")
        raise BoxAPIError("folder_search_limit_reached")

    def upload(self, folder_id: str, path: Path, *, existing_policy="error") -> dict:
        _validate_upload(path, existing_policy)
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha1").hexdigest()
        found = [
            item
            for item in self.children(folder_id)
            if item.get("name", "").casefold() == path.name.casefold()
        ]
        if len(found) > 1 or (found and found[0].get("type") != "file"):
            raise BoxAPIError("ambiguous_destination")
        existing = found[0] if found else None
        if existing and existing.get("sha1", "").lower() == digest:
            return {**existing, "upload_status": "skipped_identical"}
        if existing and existing_policy != "version":
            raise BoxAPIError("existing_file_requires_version_policy")
        endpoint = (
            f"/files/{_id(existing['id'])}/content" if existing else "/files/content"
        )
        attributes = (
            {"name": path.name}
            if existing
            else {"name": path.name, "parent": {"id": _id(folder_id)}}
        )
        headers = {}
        if existing:
            if not existing.get("etag"):
                raise BoxAPIError("missing_version_guard")
            headers["If-Match"] = existing["etag"]
        with path.open("rb") as stream:
            result = self.request(
                "POST",
                endpoint,
                upload=True,
                headers=headers,
                files=[
                    ("attributes", (None, json.dumps(attributes), "application/json")),
                    ("file", (path.name, stream, "application/octet-stream")),
                ],
            )
        try:
            file_id = _id(result["entries"][0]["id"])
            verified = self.request(
                "GET",
                f"/files/{file_id}",
                params={"fields": "id,type,name,sha1,size,parent"},
            )
        except (KeyError, IndexError, TypeError, BoxAPIError):
            raise BoxAPIError("write_outcome_unknown") from None
        if (
            verified.get("sha1", "").lower() != digest
            or verified.get("parent", {}).get("id") != folder_id
            or verified.get("name") != path.name
        ):
            raise BoxAPIError("upload_verification_failed")
        return {**verified, "upload_status": "versioned" if existing else "created"}

    def shared_link(self, kind: str, item_id: str, access: str) -> str:
        if kind not in {"file", "folder"}:
            raise BoxAPIError("invalid_item_type")
        item = self.request(
            "GET", f"/{kind}s/{_id(item_id)}", params={"fields": "shared_link"}
        )
        link = item.get("shared_link")
        if link:
            if link.get("effective_access", link.get("access")) != access:
                raise BoxAPIError("existing_shared_access_mismatch")
            return link["url"]
        if access != "invited":
            raise BoxAPIError("shared_access_requires_separate_approval")
        self.request(
            "PUT", f"/{kind}s/{item_id}", json={"shared_link": {"access": "invited"}}
        )
        item = self.request(
            "GET", f"/{kind}s/{item_id}", params={"fields": "shared_link"}
        )
        link = item.get("shared_link") or {}
        if link.get("effective_access", link.get("access")) != access or not link.get(
            "url"
        ):
            raise BoxAPIError("shared_link_verification_failed")
        return link["url"]

    def download(
        self, item: dict, directory: Path, *, max_bytes=250 * 1024 * 1024
    ) -> Path:
        name = _name(item["name"])
        if int(item.get("size", max_bytes + 1)) > max_bytes:
            raise BoxAPIError("download_size_limit")
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / name
        if destination.exists():
            raise BoxAPIError("download_destination_exists")
        response = None
        temporary = None
        try:
            response = self.session.get(
                f"https://api.box.com/2.0/files/{_id(item['id'])}/content",
                headers={
                    "Authorization": f"Bearer {self.token()}",
                    **self.shared_context,
                },
                stream=True,
                allow_redirects=False,
                timeout=(10, 120),
            )
            for _ in range(5):
                if response.status_code not in {301, 302, 303, 307, 308}:
                    break
                location = response.headers.get("Location", "")
                parsed = urlsplit(location)
                if (
                    parsed.scheme != "https"
                    or not (parsed.hostname or "").endswith(".boxcloud.com")
                    or parsed.username
                    or parsed.password
                    or parsed.port
                ):
                    raise BoxAPIError("unsafe_download_redirect")
                response.close()
                # A fresh unauthenticated session: never forward bearer/cookies to CDN.
                response = requests.get(
                    location, stream=True, allow_redirects=False, timeout=(10, 120)
                )
            if response.status_code != 200:
                raise BoxAPIError(f"box_download_http_{response.status_code}")
            fd, temporary = tempfile.mkstemp(dir=directory)
            digest = hashlib.sha1()
            count = 0
            with os.fdopen(fd, "wb") as output:
                for block in response.iter_content(64 * 1024):
                    count += len(block)
                    if count > max_bytes:
                        raise BoxAPIError("download_size_limit")
                    digest.update(block)
                    output.write(block)
            if (
                count != item.get("size")
                or digest.hexdigest() != item.get("sha1", "").lower()
            ):
                raise BoxAPIError("download_integrity_failed")
            # Hard link is atomic and refuses an existing target.
            os.link(temporary, destination)
            return destination
        except requests.RequestException:
            raise BoxAPIError("download_network_error") from None
        finally:
            if response is not None:
                response.close()
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)


def client_from_env(*, write=False) -> BoxArtifactClient:
    token_path = os.getenv("BOX_API_TOKEN_FILE", "")
    if not token_path:
        raise BoxAPIError("BOX_API_TOKEN_FILE_required")
    if write and os.getenv("BOX_API_WRITE_ENABLED") != "1":
        raise BoxAPIError("dedicated_write_oauth_required")
    provider = OAuthTokenFile(token_path)
    data = json.loads(provider.path.read_text())
    if (
        data.get("actor_verified") is not True
        or data.get("expected_login", "").casefold()
        != os.getenv("BOX_API_EXPECTED_LOGIN", "").casefold()
    ):
        raise BoxAPIError("verified_artifact_oauth_required")
    return BoxArtifactClient(
        provider,
        expected_login=os.getenv("BOX_API_EXPECTED_LOGIN", ""),
        allow_write=write,
    )


def upload_files_to_box_folder_via_api_sync(
    *,
    parent_folder_url,
    target_subfolder_name="",
    nested_subfolder_name="",
    file_paths,
    create_shared_link=False,
    shared_link_access="invited",
    shared_link_target="file",
    existing_policy="error",
    client=None,
):
    # Validate the entire batch before external writes.
    paths = [Path(p).expanduser().resolve() for p in file_paths]
    if not paths:
        return []
    if len({p.name.casefold() for p in paths}) != len(paths):
        raise BoxAPIError("duplicate_batch_file_names")
    for path in paths:
        _validate_upload(path, existing_policy)
    if shared_link_target not in {"file", "folder"} or shared_link_access not in {
        "invited",
        "company",
        "open",
    }:
        raise BoxAPIError("invalid_shared_link_options")
    if existing_policy not in {"error", "version"}:
        raise BoxAPIError("invalid_existing_policy")
    if not all(
        isinstance(v, str) for v in (target_subfolder_name, nested_subfolder_name)
    ):
        raise BoxAPIError("subfolder_name_must_be_string")
    levels = [v.strip() for v in (target_subfolder_name, nested_subfolder_name)]
    for level in levels:
        if level:
            _name(level)
    client = client or client_from_env(write=True)
    parent = client.resolve(parent_folder_url)
    if parent.get("type") != "folder":
        raise BoxAPIError("destination_must_be_folder")
    folder_id = _id(parent["id"])
    created = []
    for level in levels:
        if not level:
            created.append(False)
            continue
        matches = [
            i
            for i in client.children(folder_id)
            if i.get("name", "").casefold() == level.casefold()
        ]
        if matches:
            if len(matches) != 1 or matches[0].get("type") != "folder":
                raise BoxAPIError("ambiguous_destination")
            folder_id = _id(matches[0]["id"])
            created.append(False)
        else:
            expected_parent_id = folder_id
            folder = client.request(
                "POST", "/folders", json={"name": level, "parent": {"id": folder_id}}
            )
            try:
                folder_id = _id(folder["id"])
                verified = client.request(
                    "GET", f"/folders/{folder_id}", params={"fields": "name,parent"}
                )
                if (
                    verified.get("name") != level
                    or verified.get("parent", {}).get("id") != expected_parent_id
                ):
                    raise BoxAPIError("folder_verification_failed")
            except (KeyError, BoxAPIError):
                raise BoxAPIError("write_outcome_unknown") from None
            created.append(True)
    results = []
    for path in paths:
        item = client.upload(folder_id, path, existing_policy=existing_policy)
        result = dict(
            uploaded_file_name=path.name,
            target_subfolder_name=levels[0],
            nested_subfolder_name=levels[1],
            folder_created=created[0],
            nested_folder_created=created[1],
            upload_mode="box-api",
            output_dir=str(path.parent),
            web_url=f"https://app.box.com/file/{item['id']}",
            file_id=item["id"],
            sha1=item["sha1"],
            upload_status=item["upload_status"],
        )
        results.append(result)
        if create_shared_link and shared_link_target == "file":
            result.update(
                _shared_link_result(client, "file", item["id"], shared_link_access)
            )
    if create_shared_link and shared_link_target == "folder":
        link_result = _shared_link_result(
            client, "folder", folder_id, shared_link_access
        )
        for result in results:
            result.update(
                {"folder_" + key: value for key, value in link_result.items()}
            )
    return results


def _shared_link_result(client, kind, item_id, access):
    # Content delivery and link publication are independent outcomes. Retain
    # upload IDs even if link policy/transport/readback fails.
    try:
        url = client.shared_link(kind, item_id, access)
        return dict(
            shared_url=url,
            shared_link_access=access,
            shared_link_status="verified",
            shared_link_error="",
        )
    except BoxAPIError as exc:
        return dict(
            shared_url="",
            shared_link_access=access,
            shared_link_status="failed",
            shared_link_error=str(exc),
        )


async def download_from_box_via_api(
    *, url, download_dir, expected_folder_file_names=None, client=None
):
    def run():
        api = client or client_from_env()
        item = api.resolve(url)
        directory = Path(download_dir).expanduser().resolve()
        if item.get("type") == "file":
            return api.download(item, directory)
        if item.get("type") != "folder" or not expected_folder_file_names:
            raise BoxAPIError("folder_download_requires_explicit_file_names")
        destination = directory / "box-download.zip"
        if destination.exists():
            raise BoxAPIError("download_destination_exists")
        names = [_name(n) for n in expected_folder_file_names]
        if len(set(names)) != len(names):
            raise BoxAPIError("duplicate_expected_names")
        children = api.children(item["id"])
        files = []
        for name in names:
            matches = [
                i for i in children if i.get("name") == name and i.get("type") == "file"
            ]
            if len(matches) != 1:
                raise BoxAPIError("expected_file_missing_or_ambiguous")
            files.append(matches[0])
        if sum(int(i["size"]) for i in files) > 250 * 1024 * 1024:
            raise BoxAPIError("download_size_limit")
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=directory) as staging:
            downloaded = [api.download(i, Path(staging)) for i in files]
            archive = Path(staging) / "box-download.zip"
            with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
                for path in downloaded:
                    bundle.write(path, path.name)
            os.link(archive, destination)
            return destination

    return await asyncio.to_thread(run)
