import hashlib
import json
from unittest.mock import Mock

import pytest

from megaton_lib.docs_assets import DriveImageStager
from megaton_lib.docs_client import DocsEditError
from megaton_lib.docs_mutation_io import create_receipt


def setup_asset(tmp_path):
    from PIL import Image
    path = tmp_path / "private.png"
    Image.new("RGB", (4, 4)).save(path)
    asset = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "operation_id": "image"}
    receipt = {"plan_digest": "approved", "assets": [], "ok": True}
    journal = tmp_path / "receipt.json"
    create_receipt(journal, receipt)
    return asset, receipt, journal


def test_public_access_is_opt_in(tmp_path):
    asset, receipt, journal = setup_asset(tmp_path)
    service = Mock()
    stager = DriveImageStager(service, folder_id="folder")
    with pytest.raises(DocsEditError, match="approval"):
        stager.stage(asset, receipt, journal)
    service.files.assert_not_called()


def test_temporary_copy_is_shared_and_deleted_not_original(tmp_path):
    asset, receipt, journal = setup_asset(tmp_path)
    service = Mock()
    service.files.return_value.create.return_value.execute.return_value = {"id": "temporary"}
    service.permissions.return_value.create.return_value.execute.return_value = {"id": "permission"}
    stager = DriveImageStager(service, folder_id="folder", allow_public=True)
    assert stager.stage(asset, receipt, journal).endswith("id=temporary")
    assert receipt["assets"][0]["file_id"] == "temporary"
    stager.cleanup(receipt, journal)
    assert receipt["cleanup_status"] == "complete"
    service.files.return_value.delete.assert_called_once_with(fileId="temporary", supportsAllDrives=True)
    service.files.return_value.create.return_value.execute.assert_called_once_with(num_retries=0)
    service.permissions.return_value.create.return_value.execute.assert_called_once_with(num_retries=0)
    assert "image_uri" not in json.dumps(receipt)


def test_unknown_upload_is_journaled_and_never_retried(tmp_path):
    asset, receipt, journal = setup_asset(tmp_path)
    service = Mock()
    service.files.return_value.create.return_value.execute.side_effect = TimeoutError("secret")
    stager = DriveImageStager(service, folder_id="folder", allow_public=True)
    with pytest.raises(TimeoutError):
        stager.stage(asset, receipt, journal)
    stager.cleanup(receipt, journal)
    assert not receipt["ok"] and receipt["cleanup_status"] == "unknown"
    assert receipt["assets"][0]["upload_status"] == "unknown"
    assert "secret" not in journal.read_text()
    service.files.return_value.create.return_value.execute.assert_called_once()


def test_cleanup_failure_is_not_success(tmp_path):
    _, receipt, journal = setup_asset(tmp_path)
    receipt["assets"] = [{"file_id": "temporary", "cleanup_status": "pending"}]
    service = Mock()
    service.files.return_value.delete.return_value.execute.side_effect = TimeoutError("secret")
    DriveImageStager(service, folder_id="folder").cleanup(receipt, journal)
    assert not receipt["ok"] and receipt["cleanup_status"] == "unknown"
    assert "secret" not in journal.read_text()


def test_staging_rejects_service_account_without_auth(tmp_path):
    token = tmp_path / "sa.json"
    token.write_text('{"type":"service_account","private_key":"secret"}')
    with pytest.raises(DocsEditError, match="user OAuth"):
        DriveImageStager.from_oauth_file(token, expected_email="a@b.test", folder_id="folder")
