from __future__ import annotations

import json

from megaton_lib.validation.followups import finalize_followup_verification


def test_finalize_followup_verification_updates_file_and_pending_store(tmp_path):
    verification_path = tmp_path / "verification.json"
    pending_path = tmp_path / "pending.json"

    verification_payload = {
        "executionMode": "live",
        "status": "pending",
    }
    verification_path.write_text(
        json.dumps(verification_payload, ensure_ascii=False),
        encoding="utf-8",
    )
    pending_path.write_text(
        json.dumps(
            {
                "_comment": "test",
                "verifications": [
                    {
                        "id": "task-1",
                        "verification_file": str(verification_path),
                        "verification_type": "specialcode",
                        "status": "pending",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    updated_task = finalize_followup_verification(
        verification_payload,
        json_path=verification_path,
        pending_file=pending_path,
        verification_type="specialcode",
        result="pass",
        project="wws-analysis",
        scenario="specialcode_aa_followup",
        extra={"rsid": "wacoal-all"},
    )

    assert updated_task is not None
    assert updated_task["status"] == "completed"
    assert updated_task["result"] == "pass"

    saved_verification = json.loads(verification_path.read_text(encoding="utf-8"))
    assert saved_verification["aa_followup_metadata"]["project"] == "wws-analysis"
    assert saved_verification["aa_followup_metadata"]["scenario"] == "specialcode_aa_followup"
    assert saved_verification["aa_followup_metadata"]["rsid"] == "wacoal-all"

    saved_pending = json.loads(pending_path.read_text(encoding="utf-8"))
    assert saved_pending["verifications"][0]["status"] == "completed"
    assert saved_pending["verifications"][0]["result"] == "pass"
