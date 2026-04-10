from __future__ import annotations

import json

from megaton_lib.validation.aa_api import (
    build_adobe_analytics_client,
    resolve_adobe_credentials_path,
    run_aa_api_followup_verifier,
)


def test_resolve_adobe_credentials_path_prefers_explicit(tmp_path):
    explicit = tmp_path / "adobe.json"
    explicit.write_text("{}", encoding="utf-8")

    resolved = resolve_adobe_credentials_path(explicit)

    assert resolved == explicit.resolve()


def test_build_adobe_analytics_client_reads_json_and_sets_runtime_env(tmp_path, monkeypatch):
    creds = tmp_path / "adobe.json"
    creds.write_text(
        json.dumps(
            {
                "client_id": "cid",
                "client_secret": "secret",
                "org_id": "ORG@AdobeOrg",
                "company_id": "company",
            }
        ),
        encoding="utf-8",
    )
    cache = tmp_path / "cache.json"

    captured = {}

    class DummyClient:
        def __init__(self, config) -> None:
            captured["config"] = config

    monkeypatch.setattr("megaton_lib.validation.aa_api.AdobeAnalyticsClient", DummyClient)

    client = build_adobe_analytics_client(
        adobe_config_path=creds,
        company_id="",
        org_id="",
        rsid="suite",
        token_cache_file=cache,
        client_id_env="TEST_AA_CLIENT_ID",
        client_secret_env="TEST_AA_CLIENT_SECRET",
        org_id_env="TEST_AA_ORG_ID",
    )

    assert isinstance(client, DummyClient)
    assert captured["config"].company_id == "company"
    assert captured["config"].rsid == "suite"
    assert captured["config"].token_cache_file == str(cache.resolve())
    assert captured["config"].client_id_env == "TEST_AA_CLIENT_ID"


def test_run_aa_api_followup_verifier_loads_and_finalizes(tmp_path):
    verification_path = tmp_path / "verification.json"
    verification_path.write_text(
        json.dumps({"executionMode": "live", "status": "pending"}),
        encoding="utf-8",
    )
    pending_path = tmp_path / "pending.json"
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
            }
        ),
        encoding="utf-8",
    )

    verification, task = run_aa_api_followup_verifier(
        json_path=verification_path,
        pending_file=pending_path,
        verification_type="specialcode",
        project="wws-analysis",
        scenario="specialcode_aa_followup",
        verifier=lambda payload: ("pass", {"rsid": "wacoal-all", "ok": payload["status"] == "pending"}),
    )

    assert verification["aa_followup_metadata"]["rsid"] == "wacoal-all"
    assert verification["aa_followup_metadata"]["ok"] is True
    assert task is not None
    assert task["status"] == "completed"
    assert task["result"] == "pass"
