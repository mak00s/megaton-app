"""Reusable storefront validation helpers for auth, beacon capture, and timing."""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from playwright.sync_api import Page

from .adobe_analytics import dump_digital_data


JST = timezone(timedelta(hours=9))
ADOBE_BEACON_HOSTS = ("edge.adobedc.net", "s-adobe.wacoal.jp")


@dataclass
class CapturedBeacons:
    """Container for captured Adobe beacon requests plus derived ECID."""

    beacons: list[dict[str, Any]] = field(default_factory=list)
    ecid: str = ""

    def add(self, url: str, body: dict[str, Any] | None) -> None:
        self.beacons.append({"url": url, "body": body})

    def find_by_pagename(self, pattern: str) -> dict[str, Any] | None:
        """Find the first captured beacon whose pageName matches ``pattern``."""
        for beacon in self.beacons:
            body = beacon.get("body")
            if not isinstance(body, dict):
                continue
            for event in body.get("events", []):
                xdm = event.get("xdm", {})
                page_name = xdm.get("web", {}).get("webPageDetails", {}).get("name", "")
                if page_name and re.search(pattern, page_name):
                    return self._extract_analytics(event)
        return None

    def _extract_analytics(self, event: dict[str, Any]) -> dict[str, Any]:
        """Extract page-level analytics details from one edge event."""
        xdm = event.get("xdm", {})
        data_payload = event.get("data", {}) or xdm.get("data", {})
        analytics = xdm.get("_experience", {}).get("analytics", {})
        result: dict[str, Any] = {
            "pageName": xdm.get("web", {}).get("webPageDetails", {}).get("name", ""),
            "eventType": xdm.get("eventType", ""),
        }

        if analytics.get("productString"):
            result["products"] = analytics["productString"]

        for key in ("event1to100", "event101to200", "event201to300"):
            ev_data = analytics.get(key)
            if ev_data and isinstance(ev_data, dict):
                result["events"] = ev_data
                break

        custom_dims = analytics.get("customDimensions", {})
        if custom_dims.get("eVars"):
            result["eVars"] = custom_dims["eVars"]
        if custom_dims.get("props"):
            result["props"] = custom_dims["props"]
        if data_payload:
            result["data"] = data_payload
            if data_payload.get("currentTime"):
                result["currentTime"] = data_payload["currentTime"]
            if data_payload.get("currentDate"):
                result["currentDate"] = data_payload["currentDate"]

        if xdm.get("commerce"):
            result["commerce"] = xdm["commerce"]

        if xdm.get("productListItems"):
            items = xdm["productListItems"]
            result["productListItems"] = items
            merch: dict[str, dict[str, Any]] = {}
            for idx, item in enumerate(items):
                item_evars = (
                    item.get("_experience", {})
                    .get("analytics", {})
                    .get("customDimensions", {})
                    .get("eVars", {})
                )
                if item_evars:
                    merch[f"item[{idx}]"] = item_evars
            if merch:
                result["merchandisingEVars"] = merch

        identity_map = xdm.get("identityMap", {})
        renkeiid_list = identity_map.get("renkeiid", [])
        if renkeiid_list and renkeiid_list[0].get("id"):
            result["renkeiid"] = renkeiid_list[0]["id"]

        return result


def load_json_credentials(path: Path) -> dict[str, Any]:
    """Load a local credentials JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Credentials file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def next_aa_reflection_time(now: datetime) -> datetime:
    """Return the next expected AA batch reflection time in JST."""
    base = now.replace(second=0, microsecond=0)
    if base.minute < 30:
        bucket_close = base.replace(minute=30)
    else:
        bucket_close = base.replace(minute=0) + timedelta(hours=1)
    return bucket_close + timedelta(minutes=30)


def _matches_any_host(url: str, hosts: Sequence[str]) -> bool:
    return any(host in url for host in hosts)


def _parse_request_json(request) -> dict[str, Any] | None:
    body = request.post_data
    if not body:
        return None
    if isinstance(body, str):
        return json.loads(body)
    return json.loads(body.decode())


def _extract_ecid_from_interact_response(
    response,
    *,
    beacon_hosts: Sequence[str],
) -> str:
    url = response.url
    if "/interact" not in url or not _matches_any_host(url, beacon_hosts):
        return ""
    try:
        body = response.json()
    except Exception:
        return ""

    for handle in body.get("handle", []):
        for payload in handle.get("payload", []):
            namespace = payload.get("namespace", {})
            if namespace.get("code") == "ECID" and payload.get("id"):
                return str(payload["id"])
    return ""


def setup_storefront_validation_page(
    page: Page,
    *,
    domain: str | None = None,
    basic_auth: Mapping[str, str] | None = None,
    embed_override: str | None = None,
    beacons: CapturedBeacons | None = None,
    beacon_hosts: Sequence[str] = ADOBE_BEACON_HOSTS,
) -> None:
    """Attach common storefront validation routes and Adobe beacon capture."""
    if domain and basic_auth:
        auth_header = base64.b64encode(
            f"{basic_auth['username']}:{basic_auth['password']}".encode(),
        ).decode()

        def handle_auth(route):  # type: ignore[no-untyped-def]
            headers = route.request.headers.copy()
            headers["Authorization"] = f"Basic {auth_header}"
            route.continue_(headers=headers)

        page.route(f"**://{domain}/**", handle_auth)

    if embed_override:
        override_js: bytes | None = None

        def handle_embed(route):  # type: ignore[no-untyped-def]
            nonlocal override_js
            if "launch-" not in route.request.url:
                route.continue_()
                return

            if override_js is None:
                import urllib.request

                with urllib.request.urlopen(embed_override, timeout=30) as resp:
                    override_js = resp.read()
            route.fulfill(body=override_js, content_type="application/javascript")

        page.route("**/launch-*.js", handle_embed)

    if beacons is None:
        return

    def on_request(request):  # type: ignore[no-untyped-def]
        url = request.url
        if not _matches_any_host(url, beacon_hosts):
            return
        try:
            parsed = _parse_request_json(request)
            if parsed is not None:
                beacons.add(url, parsed)
        except Exception:
            beacons.add(url, None)

    def on_response(response):  # type: ignore[no-untyped-def]
        ecid = _extract_ecid_from_interact_response(response, beacon_hosts=beacon_hosts)
        if ecid:
            beacons.ecid = ecid

    page.on("request", on_request)
    page.on("response", on_response)


__all__ = [
    "ADOBE_BEACON_HOSTS",
    "CapturedBeacons",
    "JST",
    "dump_digital_data",
    "load_json_credentials",
    "next_aa_reflection_time",
    "setup_storefront_validation_page",
]
