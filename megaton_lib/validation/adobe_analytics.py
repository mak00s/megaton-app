"""Reusable Playwright-based Adobe Analytics beacon validation helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .playwright_pages import (
    TagsLaunchOverride,
    build_tags_launch_override,
    capture_satellite_info,
    run_page,
)
from .metadata import build_validation_run_metadata


def load_validation_config(config_path: Path) -> dict:
    """Load a validation config JSON file."""
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


def _parse_form_encoded_values(payload: str | bytes | None) -> dict[str, str]:
    """Parse a querystring-style payload into a flat string dict."""
    if not payload:
        return {}
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError:
            return {}

    payload = payload.lstrip("?")
    if "=" not in payload:
        return {}

    params: dict[str, str] = {}
    for key, values in parse_qs(payload, keep_blank_values=True).items():
        params[key] = values[0] if values else ""
    return params


def parse_appmeasurement_url(url: str, post_data: str | bytes | None = None) -> dict[str, str]:
    """Extract Adobe Analytics variables from a ``b/ss/`` beacon request."""
    parsed = urlparse(url)
    params: dict[str, str] = {}

    path_parts = parsed.path.split("/")
    try:
        bss_idx = path_parts.index("ss")
        if bss_idx + 1 < len(path_parts):
            params["rsid"] = path_parts[bss_idx + 1]
    except ValueError:
        pass

    params.update(_parse_form_encoded_values(parsed.query))
    params.update(_parse_form_encoded_values(post_data))

    return params


@dataclass
class AppMeasurementCapture:
    """Mutable collector for parsed AppMeasurement `b/ss` beacons."""

    beacons: list[Any] = field(default_factory=list)
    parser: Callable[[Any], Any | None] | None = None

    def attach(self, page: Any) -> Callable[[Any], None]:
        """Attach request capture to a Playwright page."""
        return attach_appmeasurement_capture(page, self.beacons, parser=self.parser)

    def checkpoint(self) -> int:
        """Return a checkpoint index for later incremental slicing."""
        return len(self.beacons)

    def since(self, start_index: int) -> list[Any]:
        """Return beacons captured after `start_index`."""
        return slice_appmeasurement_beacons(self.beacons, start_index)

    def collect_after(
        self,
        action: Callable[[], Any],
        *,
        page: Any | None = None,
        wait_ms: int = 0,
    ) -> tuple[Any, list[Any]]:
        """Run one action and return both its result and new beacons since the start."""
        checkpoint = self.checkpoint()
        result = action()
        if page is not None and wait_ms > 0:
            page.wait_for_timeout(wait_ms)
        return result, self.since(checkpoint)

    def snapshot(self) -> list[Any]:
        """Return a shallow copy of all captured beacons."""
        return list(self.beacons)

    def clear(self) -> None:
        """Discard all captured beacons."""
        self.beacons.clear()

    def wait_until_ready(
        self,
        page: Any,
        *,
        timeout_ms: int = 30_000,
        poll_ms: int = 1_000,
        settle_ms: int = 2_000,
    ) -> dict[str, int | str]:
        """Wait until a beacon fires or AppMeasurement runtime is available."""
        return wait_for_appmeasurement_ready(
            page,
            self.beacons,
            timeout_ms=timeout_ms,
            poll_ms=poll_ms,
            settle_ms=settle_ms,
        )


def execute_appmeasurement_scenario(
    page: Any,
    appmeasurement: AppMeasurementCapture,
    steps: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Execute declarative AppMeasurement steps and collect incremental beacons."""
    out: list[dict[str, Any]] = []
    for index, step in enumerate(steps, start=1):
        action = str(step.get("action", "")).strip()
        if not action:
            raise ValueError("scenario step action is required")
        name = str(step.get("name", "")).strip() or f"step{index}"
        wait_ms = int(step.get("waitMs", 0) or 0)

        if action == "goto":
            url = str(step.get("url", "")).strip()
            if not url:
                raise ValueError(f"{name}: goto step requires url")

            def _runner() -> Any:
                return page.goto(
                    url,
                    wait_until=str(step.get("waitUntil", "domcontentloaded")),
                    timeout=int(step.get("timeout", 60000)),
                )

        elif action == "click":
            selector = str(step.get("selector", "")).strip()
            if not selector:
                raise ValueError(f"{name}: click step requires selector")

            def _runner() -> Any:
                return page.click(selector, force=bool(step.get("force", False)))

        elif action == "goBack":
            def _runner() -> Any:
                return page.go_back(
                    wait_until=str(step.get("waitUntil", "domcontentloaded")),
                    timeout=int(step.get("timeout", 60000)),
                )

        elif action == "wait":
            def _runner() -> Any:
                return None

        elif action == "callback":
            callback = step.get("callback")
            if not callable(callback):
                raise ValueError(f"{name}: callback step requires callable callback")

            def _runner() -> Any:
                return callback()

        else:
            raise ValueError(f"{name}: unsupported scenario action '{action}'")

        result, beacons = appmeasurement.collect_after(
            _runner,
            page=page if wait_ms > 0 else None,
            wait_ms=wait_ms,
        )
        out.append(
            {
                "name": name,
                "action": action,
                "result": result,
                "beacons": beacons,
            }
        )
    return out


def extract_appmeasurement_request(request: Any) -> dict[str, str] | None:
    """Return parsed AppMeasurement params for a Playwright request, if any."""
    req_url = str(getattr(request, "url", "") or "")
    if "b/ss/" not in req_url:
        return None
    return parse_appmeasurement_url(req_url, getattr(request, "post_data", None))


def attach_appmeasurement_capture(
    page: Any,
    sink: list[Any],
    *,
    parser: Callable[[Any], Any | None] | None = None,
) -> Callable[[Any], None]:
    """Attach a request listener that appends parsed ``b/ss`` beacons into ``sink``."""
    parser_fn = parser or extract_appmeasurement_request

    def on_request(request: Any) -> None:
        parsed = parser_fn(request)
        if parsed is not None:
            sink.append(parsed)

    page.on("request", on_request)
    return on_request


def slice_appmeasurement_beacons(
    beacons: list[Any],
    start_index: int,
) -> list[Any]:
    """Return a shallow copy of beacons captured after ``start_index``."""
    return list(beacons[start_index:])


def wait_for_appmeasurement_ready(
    page: Any,
    beacons: list[dict[str, str]],
    *,
    timeout_ms: int = 30_000,
    poll_ms: int = 1_000,
    settle_ms: int = 2_000,
) -> dict[str, int | str]:
    """Wait until a beacon fires or `_satellite`/`s` are available."""
    elapsed_ms = 0
    while elapsed_ms < timeout_ms:
        if beacons:
            if settle_ms > 0:
                page.wait_for_timeout(settle_ms)
            return {"status": "beacon", "elapsedMs": elapsed_ms}

        sat_ok = page.evaluate(
            "() => typeof _satellite !== 'undefined' && typeof s !== 'undefined'"
        )
        if sat_ok:
            if settle_ms > 0:
                page.wait_for_timeout(settle_ms)
            return {"status": "satellite", "elapsedMs": elapsed_ms}

        page.wait_for_timeout(poll_ms)
        elapsed_ms += poll_ms

    return {"status": "timeout", "elapsedMs": timeout_ms}


def parse_edge_body(body: str | bytes | None) -> dict | None:
    """Parse a Web SDK edge request body."""
    if not body:
        return None
    try:
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def extract_analytics_from_edge(body: dict) -> dict | None:
    """Extract Adobe Analytics variables from a Web SDK edge payload."""
    if not body or not isinstance(body, dict):
        return None

    for ev in body.get("events", []):
        xdm = ev.get("xdm", {})
        xdm_data = ev.get("data", {}) or xdm.get("data", {})
        event_type = xdm.get("eventType", "")
        analytics = xdm.get("_experience", {}).get("analytics", {})
        commerce = xdm.get("commerce", {})
        product_list_items = xdm.get("productListItems", [])

        if not analytics and not commerce:
            continue

        result: dict = {"eventType": event_type}

        page_name = xdm.get("web", {}).get("webPageDetails", {}).get("name", "")
        if page_name:
            result["pageName"] = page_name

        channel = analytics.get("channel", "")
        if channel:
            result["channel"] = channel

        products = analytics.get("productString", "")
        if products:
            result["products"] = products

        for key in ("event1to100", "event101to200", "event201to300"):
            ev_data = analytics.get(key)
            if ev_data and isinstance(ev_data, dict):
                result["events"] = ev_data

        custom_dims = analytics.get("customDimensions", {})
        evars = custom_dims.get("eVars", {})
        props = custom_dims.get("props", {})
        if evars:
            result["eVars"] = evars
        if props:
            result["props"] = props
        if xdm_data:
            result["data"] = xdm_data
            if xdm_data.get("currentTime"):
                result["currentTime"] = xdm_data["currentTime"]
            if xdm_data.get("currentDate"):
                result["currentDate"] = xdm_data["currentDate"]

        if commerce:
            result["commerce"] = commerce

        if product_list_items:
            result["productListItems"] = product_list_items
            merch_evars: dict[str, dict] = {}
            for idx, item in enumerate(product_list_items):
                item_evars = (
                    item.get("_experience", {})
                    .get("analytics", {})
                    .get("customDimensions", {})
                    .get("eVars", {})
                )
                if item_evars:
                    merch_evars[f"item[{idx}]"] = item_evars
            if merch_evars:
                result["merchandisingEVars"] = merch_evars

        return result

    return None


def _build_steps_from_legacy(config: dict) -> list[dict]:
    return [{"action": "goto", "url": config["url"]}]


def _normalize_patterns(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [item for item in value if item]


def _matches_any_pattern(value: str, patterns: list[str]) -> bool:
    if not patterns:
        return True
    return any(re.search(pattern, value) for pattern in patterns)


def _resolve_entry_url(steps: list[dict]) -> str:
    for step in steps:
        if step.get("action") == "goto" and step.get("url"):
            return str(step["url"])
    return ""


def _build_tags_override_config(config: dict) -> TagsLaunchOverride | None:
    tags_override = config.get("tagsOverride")
    if isinstance(tags_override, str):
        tags_override = {"launchUrl": tags_override}
    elif tags_override is not None and not isinstance(tags_override, dict):
        raise ValueError("tagsOverride must be an object or string")

    legacy_dev_embed = str(config.get("devEmbed", "")).strip()
    if not tags_override and legacy_dev_embed:
        tags_override = {"launchUrl": legacy_dev_embed, "mode": "launch_env"}

    if not tags_override:
        return None
    if "launchUrl" not in tags_override and legacy_dev_embed:
        tags_override = dict(tags_override)
        tags_override["launchUrl"] = legacy_dev_embed

    return build_tags_launch_override(tags_override, require=True, label="tagsOverride")


def execute_playwright_steps(page, steps: list[dict]) -> None:
    """Execute a simple navigation script on a Playwright page."""
    for i, step in enumerate(steps):
        action = step["action"]
        desc = step.get("description", "")
        label = f"  Step {i + 1}: {action}"
        if desc:
            label += f" ({desc})"

        if action == "goto":
            url = step["url"]
            print(f"{label} -> {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(step.get("waitMs", 3000))
        elif action == "fill":
            selector = step["selector"]
            value = step["value"]
            print(f"{label}: {selector} = {value}")
            page.fill(selector, value)
        elif action == "click":
            selector = step["selector"]
            force = step.get("force", False)
            print(f"{label}: {selector}")
            page.click(selector, force=force)
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            page.wait_for_timeout(step.get("waitMs", 3000))
        elif action == "clickAndNavigate":
            selector = step["selector"]
            print(f"{label}: {selector}")
            with page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
                page.click(selector)
            print(f"    -> {page.url}")
            page.wait_for_timeout(step.get("waitMs", 3000))
        elif action == "wait":
            seconds = step.get("seconds", 5)
            print(f"{label}: {seconds}s")
            page.wait_for_timeout(seconds * 1000)
        elif action == "evaluate":
            js = step["js"]
            print(label)
            result = page.evaluate(js)
            if result:
                print(f"    -> {result}")
            page.wait_for_timeout(step.get("waitMs", 2000))
        else:
            print(f"{label}: unknown action, skipping")


def dump_digital_data(page) -> dict | None:
    """Safely serialize ``digitalData`` from the page."""
    try:
        return page.evaluate(
            "() => { try { return JSON.parse(JSON.stringify(digitalData)); }"
            " catch(e) { return null; } }"
        )
    except Exception:
        return None


def run_aa_validation(config: dict) -> dict:
    """Run Playwright AA validation and return a structured result."""
    steps = config.get("steps")
    if not steps:
        steps = _build_steps_from_legacy(config)
    entry_url = _resolve_entry_url(steps)

    wait_seconds = config.get("waitSeconds", 15)
    patterns = config.get("beaconPatterns")
    if not patterns:
        legacy = config.get("beaconPattern", "b/ss/")
        patterns = [legacy]
    edge_patterns = [p for p in patterns if p != "b/ss/"]
    expected_rsid = config.get("rsid", "")
    expected_final_url_patterns = _normalize_patterns(
        config.get("expectFinalUrlPatterns") or config.get("expectFinalUrlPattern")
    )
    expected_page_name_patterns = _normalize_patterns(
        config.get("expectPageNamePatterns") or config.get("expectPageNamePattern")
    )
    expect_final_beacon = config.get("expectFinalBeaconPageName", "")
    beacon_timeout = config.get("beaconTimeout", 30)
    tags_override_config = _build_tags_override_config(config)
    dump_dd = config.get("dumpDigitalData", False)
    wait_for_fpid = config.get("waitForFPID", False)
    page_setup = config.get("pageSetup")
    bootstrap_page = config.get("bootstrapPage") or config.get("beforeSteps")
    capture_runtime = config.get("captureRuntime") or config.get("runtimeSnapshot")

    edge_requests: list[dict] = []
    appmeasurement = AppMeasurementCapture()
    page_errors: list[str] = []
    console_errors: list[str] = []
    failed_requests: list[str] = []
    step_errors: list[str] = []
    fpid_completed = False

    basic_auth = config.get("basicAuth")
    tags_override = None
    if tags_override_config:
        if not entry_url:
            raise ValueError("tagsOverride requires at least one goto step")
        tags_override = tags_override_config

    def _run(page):
        def on_request(request):
            req_url = request.url
            if any(ep in req_url for ep in edge_patterns):
                body = parse_edge_body(request.post_data)
                edge_requests.append({
                    "url": req_url,
                    "method": request.method,
                    "body": body,
                })
            else:
                params = extract_appmeasurement_request(request)
                if params is None:
                    return
                appmeasurement.beacons.append({
                    "url": req_url,
                    "params": params,
                })

        def on_page_error(error):
            page_errors.append(str(error))

        def on_console(msg):
            text = msg.text
            if any(kw in text.lower() for kw in ("error", "timeout", "fpid")):
                console_errors.append(f"[{msg.type}] {text}")

        def on_request_failed(request):
            req_url = request.url
            noise_domains = (
                "google.com/ccm", "google-analytics.com", "googletagmanager.com",
                "apm.yahoo.co.jp", "yahoo.co.jp/rt", "doubleclick.net",
                "facebook.com", "facebook.net", "meta.com",
                "line.me", "line-scdn.net",
                "criteo.com", "criteo.net",
                "dm.slim02.jp", "eagle-insight.com",
                "tr.line.me", "impact-ad.jp",
                "i.yimg.jp", "yads.c.yimg.jp",
            )
            if any(nd in req_url for nd in noise_domains):
                return
            failed_requests.append(f"{request.method} {req_url} ({request.failure})")

        def on_response(response):
            nonlocal fpid_completed
            if "/fpid/" in response.url:
                fpid_completed = True

        page.on("request", on_request)
        page.on("pageerror", on_page_error)
        page.on("console", on_console)
        page.on("requestfailed", on_request_failed)
        page.on("response", on_response)
        if callable(page_setup):
            page_setup(page)

        auth_label = " (with BASIC auth)" if basic_auth else ""
        embed_label = " (launch override)" if tags_override else ""
        print(f"Starting validation{auth_label}{embed_label}")
        print()

        try:
            if callable(bootstrap_page):
                bootstrap_page(page)
            execute_playwright_steps(page, steps)
        except Exception as exc:
            step_errors.append(str(exc))
            print(f"  ERROR: step execution failed: {exc}")

        if not step_errors and wait_for_fpid and not fpid_completed:
            fpid_timeout = 20
            print(f"\n  Waiting up to {fpid_timeout}s for FPID response...")
            for _ in range(fpid_timeout * 2):
                if fpid_completed:
                    break
                page.wait_for_timeout(500)
            if fpid_completed:
                print("  FPID response received")
            else:
                print("  WARNING: FPID response not detected within timeout")

        if expect_final_beacon and not step_errors:
            print(f"  Waiting up to {beacon_timeout}s for beacon matching '{expect_final_beacon}'...")
            elapsed = 0.0
            poll_ms = 500
            beacon_found = False
            while elapsed < beacon_timeout:
                page.wait_for_timeout(poll_ms)
                elapsed += poll_ms / 1000
                for req in edge_requests:
                    body = req.get("body")
                    if not body or not isinstance(body, dict):
                        continue
                    for ev in body.get("events", []):
                        xdm = ev.get("xdm", {})
                        pn = xdm.get("web", {}).get("webPageDetails", {}).get("name", "")
                        if pn and re.search(expect_final_beacon, pn):
                            beacon_found = True
                            break
                    if beacon_found:
                        break
                if beacon_found:
                    print(f"  Beacon matched after {elapsed:.1f}s, waiting 2s grace period...")
                    page.wait_for_timeout(2000)
                    break
            if not beacon_found:
                print(f"  WARNING: beacon matching '{expect_final_beacon}' not found within {beacon_timeout}s")
                page.wait_for_timeout(wait_seconds * 1000)
        else:
            print(f"  Waiting {wait_seconds}s for beacons...")
            page.wait_for_timeout(wait_seconds * 1000)

        final_url = page.url
        digital_data = dump_digital_data(page) if dump_dd else None
        sat_info = capture_satellite_info(page)
        runtime = capture_runtime(page) if callable(capture_runtime) else None
        return final_url, digital_data, sat_info, runtime

    final_url, digital_data, sat_info, runtime = run_page(
        entry_url or config.get("url", ""),
        headless=config.get("headless", True),
        ignore_https_errors=bool(config.get("ignoreHttpsErrors", False)),
        basic_auth=(
            {
                "username": basic_auth["username"],
                "password": basic_auth["password"],
            }
            if basic_auth
            else None
        ),
        storage_state=config.get("storageState"),
        viewport=config.get("viewport"),
        tags_override=tags_override,
        callback=_run,
    )

    issues: list[str] = []
    bss_beacons = appmeasurement.beacons
    results: dict = {
        "url": final_url,
        "edge": {"count": len(edge_requests), "requests": []},
        "bss": {"count": len(bss_beacons), "beacons": []},
        "issues": issues,
        "pageErrors": page_errors,
        "consoleErrors": console_errors,
        "failedRequests": failed_requests,
        "satellite": sat_info,
        "stepErrors": step_errors,
    }
    results.update(
        build_validation_run_metadata(
            execution_mode="tags_override" if tags_override else "live",
            config_path=config.get("configPath"),
            scenario=config.get("scenario") or config.get("name"),
            tags_override=tags_override,
        )
    )
    if digital_data is not None:
        results["digitalData"] = digital_data
    if runtime is not None:
        results["runtime"] = runtime

    for i, req in enumerate(edge_requests):
        summary: dict = {"index": i, "method": req["method"], "url": req["url"]}
        body = req.get("body")
        if body and isinstance(body, dict):
            summary["body"] = body
            events = body.get("events", [])
            xdm_types = []
            for ev in events:
                xdm = ev.get("xdm", {})
                event_type = xdm.get("eventType", "")
                if event_type:
                    xdm_types.append(event_type)
                web = xdm.get("web", {})
                page_name = web.get("webPageDetails", {}).get("name", "")
                if page_name:
                    summary["pageName"] = page_name
            if xdm_types:
                summary["eventTypes"] = xdm_types

            analytics = extract_analytics_from_edge(body)
            if analytics:
                summary["analytics"] = analytics

        results["edge"]["requests"].append(summary)

    for i, beacon in enumerate(bss_beacons):
        beacon_summary: dict = {
            "index": i,
            "rsid": beacon["params"].get("rsid", ""),
            "pageName": beacon["params"].get("pageName", ""),
            "events": beacon["params"].get("events", ""),
            "pageUrl": beacon["params"].get("g", ""),
            "prop1": beacon["params"].get("c1", ""),
            "eVar1": beacon["params"].get("v1", ""),
            "linkName": beacon["params"].get("pev2", ""),
        }
        results["bss"]["beacons"].append(beacon_summary)

        if expected_rsid and beacon["params"].get("rsid", "") != expected_rsid:
            issues.append(
                f"b/ss beacon {i}: RSID mismatch - "
                f"expected '{expected_rsid}', got '{beacon['params'].get('rsid', '')}'"
            )

    if not edge_requests and not bss_beacons:
        issues.append("No Adobe beacons detected (neither edge.adobedc.net nor b/ss/)")

    if step_errors:
        for err in step_errors:
            issues.append(f"Step execution failed: {err}")

    if expected_final_url_patterns and not _matches_any_pattern(final_url, expected_final_url_patterns):
        issues.append(
            "Final URL did not match expected pattern(s): "
            f"{final_url}"
        )

    if expected_page_name_patterns:
        matched_edge = any(
            _matches_any_pattern(req.get("pageName", ""), expected_page_name_patterns)
            for req in results["edge"]["requests"]
        )
        matched_bss = any(
            _matches_any_pattern(beacon.get("pageName", ""), expected_page_name_patterns)
            for beacon in results["bss"]["beacons"]
        )
        if not matched_edge and not matched_bss:
            issues.append(
                "No beacon matched expected pageName pattern(s): "
                f"{expected_page_name_patterns}"
            )

    return results


def _print_analytics_detail(analytics: dict) -> None:
    for key in ("pageName", "channel", "products"):
        val = analytics.get(key)
        if val:
            print(f"      {key}: {val}")

    events = analytics.get("events")
    if events:
        event_parts = []
        for ek, ev in sorted(events.items()):
            if isinstance(ev, dict) and "value" in ev:
                event_parts.append(f"{ek}={ev['value']}")
            else:
                event_parts.append(ek)
        print(f"      events: {', '.join(event_parts)}")

    evars = analytics.get("eVars", {})
    if evars:
        for ek in sorted(evars.keys(), key=lambda x: int(x.replace("eVar", ""))):
            print(f"      {ek}: {evars[ek]}")

    props = analytics.get("props", {})
    if props:
        for pk in sorted(props.keys(), key=lambda x: int(x.replace("prop", ""))):
            print(f"      {pk}: {props[pk]}")

    commerce = analytics.get("commerce", {})
    if commerce:
        parts = []
        for ck, cv in commerce.items():
            if isinstance(cv, dict) and "value" in cv:
                parts.append(f"{ck}={cv['value']}")
            else:
                parts.append(ck)
        print(f"      commerce: {', '.join(parts)}")

    pli = analytics.get("productListItems", [])
    if pli:
        for item in pli:
            sku = item.get("SKU", "")
            name = item.get("name", "")
            qty = item.get("quantity", "")
            cat = ""
            cats = item.get("productCategories", [])
            if cats:
                cat = cats[0].get("categoryID", "")
            parts = [f"SKU={sku}"]
            if name:
                parts.append(f"name={name}")
            if cat:
                parts.append(f"category={cat}")
            if qty:
                parts.append(f"qty={qty}")
            print(f"      productListItem: {', '.join(parts)}")

    merch = analytics.get("merchandisingEVars", {})
    if merch:
        for item_key, evars in merch.items():
            evar_parts = [
                f"{k}={v}"
                for k, v in sorted(evars.items(), key=lambda x: int(x[0].replace("eVar", "")))
            ]
            print(f"      merchandising ({item_key}): {', '.join(evar_parts)}")


def print_validation_report(results: dict) -> None:
    """Print a human-readable summary for ``run_aa_validation`` output."""
    print(f"\nFinal URL: {results['url']}")
    print(f"Execution mode: {results.get('executionMode', 'live')}")
    if results.get("tagsOverride"):
        print(f"Launch override: {results['tagsOverride'].get('launchUrl', '')}")

    sat = results.get("satellite", {})
    if sat.get("hasSatellite"):
        bd = sat.get("buildDate", "?")
        print(f"_satellite build: {bd}")
    else:
        print("_satellite: NOT FOUND")

    edge = results["edge"]
    bss = results["bss"]

    if edge["count"]:
        print(f"\nEdge (Web SDK) requests: {edge['count']}")
        for req in edge["requests"]:
            types = ", ".join(req.get("eventTypes", []))
            page_name = req.get("pageName", "")
            detail = []
            if types:
                detail.append(f"eventTypes=[{types}]")
            if page_name:
                detail.append(f"pageName={page_name}")
            print(f"  [{req['index']}] {req['method']}  {' '.join(detail)}")

            analytics = req.get("analytics")
            if analytics:
                _print_analytics_detail(analytics)

    if bss["count"]:
        print(f"\nb/ss beacons: {bss['count']}")
        for beacon in bss["beacons"]:
            print(
                f"  [{beacon['index']}] rsid={beacon['rsid']}  "
                f"pageName={beacon['pageName']}  events={beacon['events']}"
            )

    if not edge["count"] and not bss["count"]:
        print("\nNo Adobe beacons detected.")

    dd = results.get("digitalData")
    if dd:
        print("\ndigitalData:")
        print(json.dumps(dd, indent=2, ensure_ascii=False)[:3000])

    if results.get("consoleErrors"):
        print(f"\nConsole errors/warnings: {len(results['consoleErrors'])}")
        for msg in results["consoleErrors"]:
            print(f"  {msg}")

    if results["pageErrors"]:
        print(f"\nPage errors: {len(results['pageErrors'])}")
        for err in results["pageErrors"]:
            print(f"  {err}")

    if results["failedRequests"]:
        print(f"\nFailed requests: {len(results['failedRequests'])}")
        for req in results["failedRequests"][:5]:
            print(f"  {req}")
        remaining = len(results["failedRequests"]) - 5
        if remaining > 0:
            print(f"  ... and {remaining} more")

    if results["issues"]:
        print(f"\nIssues: {len(results['issues'])}")
        for issue in results["issues"]:
            print(f"  * {issue}")
    else:
        print("\nAll checks passed.")
