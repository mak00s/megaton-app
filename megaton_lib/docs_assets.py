"""Explicit temporary image publication for report Docs; no general Drive API.

Only newly created copies are shared and deleted. Source-file permissions never
change. The caller must approve transient public access and the staging folder.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .docs_client import DocsEditError, _id
from .docs_mutation_io import save_receipt


class DriveImageStager:
    def __init__(self, service, *, folder_id: str, allow_public: bool = False):
        self._service = service
        self.folder_id = _id(folder_id)
        self.allow_public = allow_public

    @classmethod
    def from_oauth_file(cls, token_path, *, expected_email, folder_id, allow_public=False):
        from google.oauth2.credentials import Credentials
        from .google_workspace import build_service, refresh_user_credentials
        if not expected_email or not expected_email.strip():
            raise DocsEditError("Image staging requires an expected OAuth email.")
        payload = json.loads(Path(token_path).expanduser().read_text())
        if payload.get("type") not in (None, "authorized_user") or "private_key" in payload:
            raise DocsEditError("Image staging requires explicit user OAuth.")
        creds = refresh_user_credentials(Credentials.from_authorized_user_info(payload, [
            "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/userinfo.email"]))
        identity = build_service("oauth2", "v2", credentials=creds).userinfo().get().execute(num_retries=0)
        if not identity.get("verified_email") or identity.get("email", "").casefold() != expected_email.strip().casefold():
            raise DocsEditError("Image staging OAuth identity mismatch.")
        return cls(build_service("drive", "v3", credentials=creds), folder_id=folder_id, allow_public=allow_public)

    def stage(self, asset, receipt, receipt_path):
        if not self.allow_public:
            raise DocsEditError("Temporary public image access requires explicit approval.")
        from googleapiclient.http import MediaIoBaseUpload
        import io
        data = Path(asset["path"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != asset["sha256"]:
            raise DocsEditError("Local image changed after planning.")
        entry = {"operation_id": asset["operation_id"], "folder_id": self.folder_id,
                 "upload_status": "unknown", "cleanup_status": "pending"}
        receipt["assets"].append(entry)
        save_receipt(receipt_path, receipt)
        media_type = "image/png" if data.startswith(b"\x89PNG\r\n\x1a\n") else "image/jpeg"
        response = self._service.files().create(body={
            "name": "docs-image-" + asset["sha256"][:16], "parents": [self.folder_id],
            "appProperties": {"docsPlan": receipt["plan_digest"], "docsOperation": asset["operation_id"]},
        }, media_body=MediaIoBaseUpload(io.BytesIO(data), mimetype=media_type),
            fields="id", supportsAllDrives=True).execute(num_retries=0)
        entry.update(file_id=response["id"], upload_status="acknowledged", sharing_status="unknown")
        save_receipt(receipt_path, receipt)
        permission = self._service.permissions().create(fileId=entry["file_id"],
            body={"type": "anyone", "role": "reader", "allowFileDiscovery": False},
            fields="id", supportsAllDrives=True).execute(num_retries=0)
        entry.update(permission_id=permission["id"], sharing_status="acknowledged")
        save_receipt(receipt_path, receipt)
        return "https://drive.google.com/uc?export=view&id=" + entry["file_id"]

    def cleanup(self, receipt, receipt_path):
        for entry in receipt["assets"]:
            if not entry.get("file_id"):
                entry["cleanup_status"] = "unknown"
                continue
            try:
                # Delete only the newly uploaded copy, including its permission.
                self._service.files().delete(fileId=entry["file_id"], supportsAllDrives=True).execute(num_retries=0)
                entry["cleanup_status"] = "deleted"
            except Exception:
                entry["cleanup_status"] = "unknown"
            save_receipt(receipt_path, receipt)
        receipt["cleanup_status"] = "complete" if all(e["cleanup_status"] == "deleted" for e in receipt["assets"]) else "unknown"
        if receipt["cleanup_status"] != "complete":
            receipt.update(ok=False, next_step="Inspect temporary image resources; do not automatically upload again.")
