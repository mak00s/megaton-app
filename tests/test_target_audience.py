"""Security and evidence semantics for prefetch-only diagnostics (no live API)."""
import json
import io
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

import pytest
import requests

from megaton_lib.target_audience import (
    AudienceDiagnostics, DiagnosticError, DiagnosticSession, analyze_delivery,
    check_expiry, load_members, main, prefetch, private_json, validate_case,
)


def case():
    return {"schema_version": 1, "tenant_id": "example", "property_id": 7,
            "url": "https://shop.example.test/__validation__/audience", "mbox": "diagnostic-only",
            "audiences": {"original": 11, "copy": 22},
            "activities": [{"id": 101, "audience_id": 22, "page_audience_id": 33}]}


def body(ids=None):
    ids = ["222"] if ids is None else ids
    return {"prefetch": {"mboxes": [{"trace": {
        "aam": {"segmentIds": ids, "profile": {"segments": ids}},
        "evaluatedCampaignTargets": [{"campaignId": 101, "matchedSegmentIds": [22, 33]}]},
        "options": [{"type": "html", "content": "marker", "eventToken": "never-save",
                     "responseTokens": {"activity.id": "101"}}]}]}}


def test_positive_membership_activity_and_offer_are_separate_evidence():
    result = analyze_delivery(body(), {"original": "111", "copy": "222"}, case()["activities"])
    assert result["memberships"] == {"original": False, "copy": True}
    assert result["activities"]["101"] == {
        "evaluated": True, "pageMatched": True, "audienceMatched": True, "offerReturned": True}
    assert "never-save" not in json.dumps(result)


def test_http_200_authorization_error_is_failure_not_zero_members():
    with pytest.raises(DiagnosticError, match="debug_authorization_failed"):
        analyze_delivery({"trace": {"error": "Authorization token required"}}, {"copy": "222"}, [])


def test_missing_aam_is_unknown_but_explicit_empty_lists_mean_nonmember():
    unknown = analyze_delivery({"trace": {"aam": {}}}, {"copy": "222"}, [])
    assert unknown["memberships"]["copy"] is None
    empty = analyze_delivery(body([]), {"copy": "222"}, [])
    assert empty["aamAvailable"] and empty["memberships"]["copy"] is False


def test_disagreeing_membership_lists_stop():
    data = body()
    data["prefetch"]["mboxes"][0]["trace"]["aam"]["profile"]["segments"] = []
    with pytest.raises(DiagnosticError, match="aam_membership_lists_disagree"):
        analyze_delivery(data, {"copy": "222"}, [])


def test_unattributed_offer_is_not_asserted_as_activity_delivery():
    data = body()
    del data["prefetch"]["mboxes"][0]["options"][0]["responseTokens"]
    assert analyze_delivery(data, {}, case()["activities"])["activities"]["101"]["offerReturned"] is None


@pytest.mark.parametrize("method,url,data", [
    ("PUT", "https://mc.adobe.io/example/target/activities/101/state", {"state": "approved"}),
    ("POST", "https://mc.adobe.io/example/target/audiences", {}),
    ("POST", "https://example.tt.omtrdc.net/rest/v1/delivery", {"execute": {}}),
    ("GET", "https://example.test/b/ss/suite", None),
])
def test_network_guard_blocks_writes_beacons_execute_before_transport(method, url, data, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unsafe call reached transport")
    monkeypatch.setattr(requests.Session, "request", forbidden)
    with pytest.raises(DiagnosticError):
        DiagnosticSession(case(), "company").request(method, url, json=data)


def test_page_load_needs_explicit_opt_in_and_qa_urls_rejected():
    data = case()
    del data["mbox"]
    with pytest.raises(DiagnosticError, match="page_load_requires_explicit_opt_in"):
        prefetch(data)
    assert prefetch(data, allow_page_load=True) == {"pageLoad": {}}
    data["url"] += "?at_preview_token=synthetic"
    with pytest.raises(DiagnosticError, match="invalid_or_qa_url"):
        validate_case(data)


def test_identity_requests_do_not_reuse_cookies_or_follow_redirects(monkeypatch):
    def transport(self, method, url, *a, **kw):
        assert not list(self.cookies) and kw["allow_redirects"] is False
        self.cookies.set("identity", "synthetic-cookie")
        return object()
    monkeypatch.setattr(requests.Session, "request", transport)
    session = DiagnosticSession(case(), "company")
    session.cookies.set("old_identity", "synthetic-old-cookie")
    session.get("https://dpm.demdex.net/id")
    assert not list(session.cookies)
    session.get("https://dpm.demdex.net/id")
    with pytest.raises(DiagnosticError, match="cookies_not_allowed"):
        session.get("https://dpm.demdex.net/id", headers={"Cookie": "identity=synthetic"})


def test_member_duplicates_and_expiry_fail_closed(tmp_path):
    path = tmp_path / "members.json"
    private_json(path, [{"label": "one", "ecid": "1" * 38}, {"label": "two", "ecid": "1" * 38}])
    with pytest.raises(DiagnosticError, match="duplicate"):
        load_members(path)
    with pytest.raises(DiagnosticError, match="timestamp_requires_timezone"):
        check_expiry("2030-01-01T00:00:00")
    with pytest.raises(DiagnosticError, match="expired"):
        check_expiry("2000-01-01T00:00:00+00:00")


def test_private_output_permissions_and_symlink_refusal(tmp_path):
    path = tmp_path / "result.json"
    path.write_text("old")
    path.chmod(0o644)
    private_json(path, {"status": "ok"})
    assert path.stat().st_mode & 0o777 == 0o600
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(OSError):
        private_json(link, {"status": "overwrite"})
    assert json.loads(path.read_text()) == {"status": "ok"}


def test_probe_does_not_persist_raw_response_profile_ecid_or_token(tmp_path):
    ecid = "1" * 38
    before = {"modifiedAt": "unchanged", "profileAttributes": {"private": "sensitive-value"}}
    responses = iter([before, {"d_mid": ecid, "d_blob": "sensitive-blob", "dcs_region": 1}, body(), before])
    class Session:
        delivery = "https://example.tt.omtrdc.net/rest/v1/delivery"
        def get(self, *a, **kw):
            return SimpleNamespace(status_code=200, json=lambda: next(responses))
        def post(self, *a, **kw):
            assert "execute" not in kw["json"] and "notifications" not in kw["json"]
            assert kw["json"]["experienceCloud"]["analytics"]["logging"] == "client_side"
            return SimpleNamespace(status_code=200, json=lambda: next(responses))
    diag = object.__new__(AudienceDiagnostics)
    diag.case, diag.org, diag.allow_page_load = case(), "ORG", False
    diag.target = SimpleNamespace(get=lambda endpoint: {"token": "sensitive-property"})
    diag.session = Session()
    result = diag.probe_one({"label": "one", "ecid": ecid}, "sensitive-debug", {"copy": "222"})
    path = tmp_path / "result.json"
    private_json(path, result)
    saved = path.read_text()
    for secret in [ecid, "sensitive-value", "sensitive-blob", "sensitive-debug", "sensitive-property", "never-save"]:
        assert secret not in saved


def test_expired_cli_stops_before_authentication(tmp_path, monkeypatch, capsys):
    plan, members, output = [tmp_path / name for name in ["case.json", "members.json", "result.json"]]
    private_json(plan, case())
    private_json(members, [{"label": "one", "ecid": "1" * 38}])
    monkeypatch.setattr("megaton_lib.target_audience.AudienceDiagnostics", lambda *a, **kw: pytest.fail("auth called"))
    monkeypatch.setattr("sys.argv", ["check", "probe", "--case", str(plan), "--members", str(members),
        "--output", str(output), "--adobe-config", "unused.json", "--debug-token-stdin",
        "--expires-at", "2000-01-01T00:00:00+00:00"])
    assert main() == 1
    assert "debug_token_expired" in capsys.readouterr().out
    assert not output.exists()


def test_cli_probe_anonymized_report_and_counts(tmp_path, monkeypatch, capsys):
    plan, members, output = [tmp_path / name for name in ["case.json", "members.json", "result.json"]]
    config = case()
    config["timings"] = {"publishedAt": "2020-01-01T00:00:00+00:00"}
    private_json(plan, config)
    private_json(members, [{"label": "one", "ecid": "1" * 38}])
    class Diagnostics:
        def __init__(self, *a, **kw):
            pass
        def snapshot(self):
            return {"audiences": {name: {"targetRule": {"aam": sid}} for name, sid in [("original", "111"), ("copy", "222")]}}
        def probe_one(self, row, token, ids):
            assert token == "synthetic-debug-token-1234567890"
            return {"label": row["label"], **analyze_delivery(body(), ids, config["activities"])}
    monkeypatch.setattr("megaton_lib.target_audience.AudienceDiagnostics", Diagnostics)
    monkeypatch.setattr("sys.stdin", io.StringIO("synthetic-debug-token-1234567890"))
    expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    monkeypatch.setattr("sys.argv", ["check", "probe", "--case", str(plan), "--members", str(members),
        "--output", str(output), "--adobe-config", "unused.json", "--debug-token-stdin", "--expires-at", expiry])
    assert main(private_root=tmp_path) == 0
    saved = json.loads(output.read_text())
    assert saved["completed"] and saved["counts"] == {"original": 0, "copy": 1}
    assert saved["activityCounts"]["101"]["offerReturned"] == {"true": 1, "unknown": 0}
    assert saved["elapsedSeconds"]["publishedAt"] > 0
    combined = output.read_text() + capsys.readouterr().out
    assert "1" * 38 not in combined and "synthetic-debug-token-1234567890" not in combined
