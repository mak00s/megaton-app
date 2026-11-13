"""Read-only metadata and AAM-aware Target prefetch diagnostics.

No admin mutations, execute, notifications, QA overrides, browser navigation,
Analytics collection or persistent OAuth token cache are exposed here.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from typing import Any
from urllib.parse import urlparse, parse_qs
from uuid import uuid4

import requests

from megaton_lib.audit.config import AdobeOAuthConfig, AdobeTargetConfig
from megaton_lib.audit.providers.target import AdobeTargetClient, fetch_activity


class DiagnosticError(RuntimeError):
    def __init__(self, code: str, http: int | None = None):
        self.code, self.http = code, http
        super().__init__(code)


def aware_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        raise DiagnosticError("invalid_timestamp") from None
    if parsed.tzinfo is None:
        raise DiagnosticError("timestamp_requires_timezone")
    return parsed


def check_expiry(value: str) -> None:
    if datetime.now(timezone.utc) >= aware_time(value):
        raise DiagnosticError("debug_token_expired_or_cutoff_reached")


def validate_case(case: dict) -> None:
    if case.get("schema_version") != 1:
        raise DiagnosticError("unsupported_case_schema")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(case.get("tenant_id", ""))):
        raise DiagnosticError("invalid_tenant")
    url = urlparse(case.get("url", ""))
    query = {key.lower() for key in parse_qs(url.query)}
    if (url.scheme != "https" or not url.hostname or url.username or url.password
            or url.fragment or any(key.startswith(("at_preview", "mboxoverride", "mboxmcgvid")) for key in query)):
        raise DiagnosticError("invalid_or_qa_url")
    if not isinstance(case.get("property_id"), int) or case["property_id"] <= 0:
        raise DiagnosticError("invalid_property_id")
    if not case.get("audiences") or not all(isinstance(v, int) and v > 0 for v in case["audiences"].values()):
        raise DiagnosticError("invalid_audience_ids")
    if any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", key) for key in case["audiences"]):
        raise DiagnosticError("invalid_audience_alias")
    if case.get("mbox") is not None and (not isinstance(case["mbox"], str) or not case["mbox"].strip()):
        raise DiagnosticError("invalid_mbox")
    for activity in case.get("activities", []):
        if not isinstance(activity.get("id"), int) or activity["id"] <= 0:
            raise DiagnosticError("invalid_activity_id")
    for value in case.get("timings", {}).values():
        aware_time(value)


def load_members(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    if not isinstance(data, list) or not data or len(data) > 100:
        raise DiagnosticError("members_require_1_to_100_rows")
    rows = []
    for row in data:
        if (not isinstance(row, dict)
                or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", str(row.get("label", "")))
                or not re.fullmatch(r"[0-9]{38}", str(row.get("ecid", "")))):
            raise DiagnosticError("invalid_member_label_or_ecid")
        rows.append({"label": row["label"], "ecid": row["ecid"]})
    if len({r["ecid"] for r in rows}) != len(rows) or len({r["label"] for r in rows}) != len(rows):
        raise DiagnosticError("duplicate_member_label_or_ecid")
    return rows


def prefetch(case: dict, *, allow_page_load: bool = False) -> dict:
    if case.get("mbox"):
        return {"mboxes": [{"index": 0, "name": case["mbox"]}]}
    if not allow_page_load:
        raise DiagnosticError("page_load_requires_explicit_opt_in")
    return {"pageLoad": {}}


class DiagnosticSession(requests.Session):
    """Fail closed on anything except scoped GETs and one prefetch shape."""
    def __init__(self, case: dict, company: str, *, allow_page_load: bool = False):
        super().__init__()
        self.case, self.company, self.allow_page_load = case, company, allow_page_load
        self.delivery = f"https://{case['tenant_id']}.tt.omtrdc.net/rest/v1/delivery"

    def request(self, method, url, *args, **kwargs):
        tenant = self.case["tenant_id"]
        allowed_get = (
            url.startswith(f"https://mc.adobe.io/{tenant}/target/")
            or url.startswith(f"https://analytics.adobe.io/api/{self.company}/segments/")
            or url.startswith(f"https://{tenant}.tt.omtrdc.net/rest/v1/profiles/marketingCloudVisitorId/")
            or url == "https://dpm.demdex.net/id"
        )
        if method.upper() == "GET" and allowed_get:
            pass
        elif method.upper() == "POST" and url == self.delivery:
            data = kwargs.get("json") or {}
            if (set(data) != {"id", "context", "experienceCloud", "property", "trace", "prefetch"}
                    or data["prefetch"] != prefetch(self.case, allow_page_load=self.allow_page_load)
                    or data["context"].get("address") != {"url": self.case["url"]}
                    or set(data["context"]) - {"channel", "address", "browser", "userAgent"}
                    or data["context"].get("channel") != "web"
                    or data["context"].get("browser") != {"host": urlparse(self.case["url"]).hostname}
                    or data["experienceCloud"].get("analytics") != {"logging": "client_side"}
                    or set(data["experienceCloud"]) != {"analytics", "audienceManager"}
                    or set(data["id"]) != {"marketingCloudVisitorId"}):
                raise DiagnosticError("unsafe_delivery_payload")
        else:
            raise DiagnosticError("network_operation_not_allowed")
        if kwargs.get("cookies") or any(key.lower() == "cookie" for key in {**self.headers, **kwargs.get("headers", {})}):
            raise DiagnosticError("cookies_not_allowed")
        # VIS cookies from one ECID must never influence another ECID's lookup.
        self.cookies.clear()
        kwargs["allow_redirects"] = False
        try:
            return super().request(method, url, *args, **kwargs)
        finally:
            self.cookies.clear()


def json_response(response) -> dict:
    if response.status_code != 200:
        raise DiagnosticError("http_error", response.status_code)
    try:
        body = response.json()
    except ValueError:
        raise DiagnosticError("invalid_json_response") from None
    if not isinstance(body, dict):
        raise DiagnosticError("invalid_response_shape")
    return body


def analyze_delivery(body: dict, aam_ids: dict[str, str], activities: list[dict]) -> dict:
    traces, returned, option_count, unattributed = [], set(), 0, False

    def walk(value):
        nonlocal option_count, unattributed
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "trace":
                    traces.append(child)
                elif key == "options" and isinstance(child, list):
                    option_count += len(child)
                    for option in child:
                        activity = option.get("responseTokens", {}).get("activity.id")
                        if activity is None:
                            unattributed = True
                        else:
                            returned.add(str(activity))
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(body)
    if body.get("errors"):
        raise DiagnosticError("delivery_error")
    if not traces:
        raise DiagnosticError("trace_unavailable")
    memberships = []
    evaluated = []
    for trace in traces:
        if not isinstance(trace, dict):
            raise DiagnosticError("invalid_trace")
        if trace.get("error") or trace.get("errors"):
            code = "debug_authorization_failed" if "authoriz" in str(trace).lower() else "trace_error"
            raise DiagnosticError(code)
        aam = trace.get("aam", {})
        ids = aam.get("segmentIds")
        profile = aam.get("profile", {}).get("segments")
        if isinstance(ids, list) and isinstance(profile, list):
            left, right = {str(v) for v in ids}, {str(v) for v in profile}
            if left != right:
                raise DiagnosticError("aam_membership_lists_disagree")
            memberships.append(left)
        evaluated.extend(trace.get("evaluatedCampaignTargets", []))
    # No list is UNKNOWN; an explicitly returned empty list is a valid non-member.
    available = bool(memberships)
    if available and any(ids != memberships[0] for ids in memberships):
        raise DiagnosticError("aam_trace_memberships_disagree")
    ids = memberships[0] if available else set()
    results = {}
    for activity in activities:
        aid = str(activity["id"])
        rows = [row for row in evaluated if str(row.get("campaignId")) == aid]
        matched = {int(v) for row in rows for v in row.get("matchedSegmentIds", [])}
        unmatched = {int(v) for row in rows for v in row.get("unmatchedSegmentIds", [])}
        def condition(key):
            value = activity.get(key)
            return True if value in matched else False if value in unmatched else None
        results[aid] = {
            "evaluated": bool(rows), "pageMatched": condition("page_audience_id"),
            "audienceMatched": condition("audience_id"),
            "offerReturned": True if aid in returned else None if unattributed and option_count else False,
        }
    return {"aamAvailable": available,
            "memberships": {name: sid in ids if available else None for name, sid in aam_ids.items()},
            "activities": results, "unattributedOptions": unattributed}


class AudienceDiagnostics:
    def __init__(self, case: dict, adobe_config: Path, *, company: str = "", allow_page_load: bool = False):
        validate_case(case)
        self.case, self.allow_page_load = case, allow_page_load
        creds = json.loads(adobe_config.expanduser().read_text())
        self.company = company or creds.get("company_id", "")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", self.company):
            raise DiagnosticError("company_id_required")
        try:
            oauth = AdobeOAuthConfig(client_id=creds["client_id"], client_secret=creds["client_secret"],
                                     org_id=creds["org_id"], scopes=creds["scopes"], token_cache_file=None)
            self.target = AdobeTargetClient(AdobeTargetConfig(tenant_id=case["tenant_id"], oauth=oauth),
                                            max_retries=1, timeout_sec=30)
        except Exception:
            raise DiagnosticError("adobe_auth_failed") from None
        self.org = creds["org_id"]
        self.session = DiagnosticSession(case, self.company, allow_page_load=allow_page_load)
        self.target.session = self.session

    def snapshot(self) -> dict:
        client = self.target.with_accept_header("application/vnd.adobe.target.v3+json")
        result = {"checkedAt": datetime.now(timezone.utc).isoformat(), "audiences": {}, "activities": {}, "aaSegments": {}}
        for name, aid in self.case["audiences"].items():
            raw = client.get(f"/audiences/{aid}")
            result["audiences"][name] = {key: raw.get(key) for key in ["id", "name", "targetRule", "originMetadata", "modifiedAt"]}
        for row in self.case.get("activities", []):
            raw = fetch_activity(client, row["id"])
            result["activities"][str(row["id"])] = {key: raw.get(key) for key in ["id", "name", "state", "startsAt", "endsAt", "createdAt", "modifiedAt", "locations", "experiences", "reportingSource", "propertyIds"]}
        for sid in self.case.get("aa_segment_ids", []):
            if not re.fullmatch(r"[A-Za-z0-9_-]+", sid):
                raise DiagnosticError("invalid_aa_segment_id")
            response = self.session.get(f"https://analytics.adobe.io/api/{self.company}/segments/{sid}",
                params={"includeType": "all", "expansion": "definition,publishingStatus,modified,definitionLastModified,compatibility"},
                headers=self.target._auth.get_headers(), timeout=30)
            raw = json_response(response)
            result["aaSegments"][sid] = {key: raw.get(key) for key in ["id", "name", "rsid", "definition", "publishingStatus", "modified", "definitionLastModified", "compatibility"]}
        return result

    def probe_one(self, row: dict, token: str, aam_ids: dict[str, str]) -> dict:
        tenant = self.case["tenant_id"]
        profile_url = f"https://{tenant}.tt.omtrdc.net/rest/v1/profiles/marketingCloudVisitorId/{row['ecid']}"
        def profile():
            return json_response(self.session.get(profile_url, params={"client": tenant}, timeout=30))
        before = profile()
        identity = json_response(self.session.get("https://dpm.demdex.net/id", params={
            "d_ver": "2", "d_rtbd": "json", "d_orgid": self.org, "d_mid": row["ecid"]}, timeout=30))
        blob, region = identity.get("d_blob"), identity.get("dcs_region")
        if (str(identity.get("d_mid")) != row["ecid"] or not isinstance(blob, str) or not blob
                or not str(region).isdigit() or int(region) <= 0):
            raise DiagnosticError("identity_response_not_usable")
        prop = self.target.get(f"/properties/{self.case['property_id']}")
        if not prop.get("token"):
            raise DiagnosticError("property_token_unavailable")
        data = {"id": {"marketingCloudVisitorId": row["ecid"]},
            "context": {"channel": "web", "address": {"url": self.case["url"]}, "browser": {"host": urlparse(self.case["url"]).hostname}},
            "experienceCloud": {"analytics": {"logging": "client_side"}, "audienceManager": {"blob": blob, "locationHint": int(region)}},
            "property": {"token": prop["token"]}, "trace": {"authorizationToken": token},
            "prefetch": prefetch(self.case, allow_page_load=self.allow_page_load)}
        ua = before.get("profileAttributes", {}).get("user.UserAgent", {})
        if isinstance(ua, dict) and ua.get("value"):
            data["context"]["userAgent"] = ua["value"]
        body = json_response(self.session.post(self.session.delivery, params={"client": tenant, "sessionId": uuid4().hex}, json=data, timeout=30))
        # Read back even if debug authorization fails; never restore attributes automatically.
        after = profile()
        changed = any(before.get(k) != after.get(k) for k in ["profileAttributes", "modifiedAt"])
        if changed:
            raise DiagnosticError("profile_changed_stop")
        return {"label": row["label"], **analyze_delivery(body, aam_ids, self.case.get("activities", [])), "profileChanged": False}


def private_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Refuse symlink outputs; never replace an unrelated destination through one.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "w") as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(value, stream, ensure_ascii=False, indent=2)


def main(*, adobe_config_default: str = "", company_default: str = "", private_root: Path | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "snapshot", "probe", "token-store"])
    parser.add_argument("--case", type=Path)
    parser.add_argument("--members", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--adobe-config", default=os.getenv("ANALYSIS_ADOBE_CONFIG_PATH", adobe_config_default))
    parser.add_argument("--company-id", default=company_default)
    parser.add_argument("--allow-page-load", action="store_true")
    parser.add_argument("--token-state", type=Path)
    parser.add_argument("--debug-token-stdin", action="store_true")
    parser.add_argument("--expires-at", help="Explicit timezone-aware expiry/cutoff; never inferred from copy time")
    parser.add_argument("--keychain-service")
    parser.add_argument("--keychain-account")
    args = parser.parse_args()
    os.umask(0o077)
    report = None
    try:
        for path in [args.output, args.token_state]:
            if path and private_root and not path.resolve().is_relative_to(private_root.resolve()):
                raise DiagnosticError("output_outside_private_root")
        if args.command == "token-store":
            if not all([args.expires_at, args.token_state, args.keychain_service, args.keychain_account]):
                raise DiagnosticError("token_store_requires_expiry_state_service_account")
            check_expiry(args.expires_at)
            from megaton_lib.target_debug_keychain import store_clipboard
            store_clipboard(args.keychain_service, args.keychain_account)
            private_json(args.token_state, {"expires_at": args.expires_at, "service": args.keychain_service, "account": args.keychain_account})
            print(json.dumps({"status": "keychain_saved_and_verified", "expiresAt": args.expires_at}))
            return 0
        if not args.case:
            raise DiagnosticError("case_required")
        case = json.loads(args.case.read_text())
        validate_case(case)
        members = load_members(args.members) if args.members else []
        if args.command == "validate":
            print(json.dumps({"status": "valid", "members": len(members), "networkRequests": 0}))
            return 0
        if not args.output or not args.adobe_config:
            raise DiagnosticError("explicit_adobe_config_and_output_required")
        token, expiry = None, None
        if args.command == "probe":
            if not members:
                raise DiagnosticError("members_required")
            prefetch(case, allow_page_load=args.allow_page_load)
            if args.debug_token_stdin:
                expiry = args.expires_at
                if not expiry:
                    raise DiagnosticError("explicit_expiry_required")
                check_expiry(expiry)
                token = sys.stdin.read().strip()
            elif args.token_state:
                state = json.loads(args.token_state.read_text())
                expiry = state["expires_at"]
                check_expiry(expiry)
                from megaton_lib.target_debug_keychain import read_token
                token = read_token(state["service"], state["account"])
            else:
                raise DiagnosticError("token_state_or_stdin_required")
            if not re.fullmatch(r"[A-Za-z0-9._~+/=-]{20,4096}", token):
                raise DiagnosticError("invalid_debug_token_format")
        diag = AudienceDiagnostics(case, Path(args.adobe_config), company=args.company_id, allow_page_load=args.allow_page_load)
        snapshot = diag.snapshot()
        report = {"executionMode": "api_metadata" if token is None else "api_prefetch_only", "tagsOverride": None,
            "settingsChanged": False, "aaBeaconsSent": False, "notificationsSent": False,
            "snapshot": snapshot, "results": [], "completed": False, "membersExpected": len(members)}
        if token is not None:
            aam_ids = {}
            for name, value in snapshot["audiences"].items():
                sid = (value.get("targetRule") or {}).get("aam")
                if not str(sid).isdigit() or (value.get("originMetadata") or {}).get("deletedAtOrigin"):
                    raise DiagnosticError("audience_requires_live_aam_leaf")
                aam_ids[name] = str(sid)
            for row in members:
                check_expiry(expiry)
                report["results"].append(diag.probe_one(row, token, aam_ids))
                private_json(args.output, report)
                if not report["results"][-1]["aamAvailable"]:
                    raise DiagnosticError("aam_membership_unavailable")
        report["completed"] = True
        report["checkedAt"] = datetime.now(timezone.utc).isoformat()
        report["counts"] = {name: sum(row["memberships"][name] is True for row in report["results"]) for name in case["audiences"]}
        report["elapsedSeconds"] = {name: int((aware_time(report["checkedAt"]) - aware_time(at)).total_seconds())
                                    for name, at in case.get("timings", {}).items()}
        report["activityCounts"] = {str(activity["id"]): {
            field: {"true": sum(row["activities"][str(activity["id"])][field] is True for row in report["results"]),
                    "unknown": sum(row["activities"][str(activity["id"])][field] is None for row in report["results"])}
            for field in ["evaluated", "pageMatched", "audienceMatched", "offerReturned"]}
            for activity in case.get("activities", [])}
        private_json(args.output, report)
        print(json.dumps({"status": "completed", "output": str(args.output), "membersChecked": len(report["results"]),
                          "counts": report["counts"], "activityCounts": report["activityCounts"], "elapsedSeconds": report["elapsedSeconds"]}))
        return 0
    except Exception as error:
        failure = {"status": "stopped", "code": error.code if isinstance(error, DiagnosticError) else type(error).__name__}
        if isinstance(error, DiagnosticError) and error.http:
            failure["http"] = error.http
        if report is not None and args.output:
            report["failure"] = failure
            private_json(args.output, report)
        print(json.dumps(failure))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
