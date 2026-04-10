from megaton_lib.validation.adobe_analytics import (
    AppMeasurementCapture,
    attach_appmeasurement_capture,
    execute_appmeasurement_scenario,
    extract_appmeasurement_request,
    parse_appmeasurement_url,
    slice_appmeasurement_beacons,
    wait_for_appmeasurement_ready,
)


def test_parse_appmeasurement_url_reads_get_query_params():
    params = parse_appmeasurement_url(
        "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0"
        "?pageName=home&events=event1&v1=abc"
    )

    assert params == {
        "rsid": "my-rsid",
        "pageName": "home",
        "events": "event1",
        "v1": "abc",
    }


def test_parse_appmeasurement_url_merges_post_body_params():
    params = parse_appmeasurement_url(
        "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0?AQB=1&ndh=1",
        "AQB=1&ndh=1&pageName=checkout&events=event10,event11&v12=member",
    )

    assert params == {
        "rsid": "my-rsid",
        "AQB": "1",
        "ndh": "1",
        "pageName": "checkout",
        "events": "event10,event11",
        "v12": "member",
    }


def test_parse_appmeasurement_url_accepts_bytes_post_body():
    params = parse_appmeasurement_url(
        "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0",
        b"pageName=order+complete&events=event42",
    )

    assert params["rsid"] == "my-rsid"
    assert params["pageName"] == "order complete"
    assert params["events"] == "event42"


def test_extract_appmeasurement_request_reads_post_body():
    class FakeRequest:
        url = "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0"
        post_data = "pageName=checkout&events=event9"

    params = extract_appmeasurement_request(FakeRequest())

    assert params == {
        "rsid": "my-rsid",
        "pageName": "checkout",
        "events": "event9",
    }


def test_attach_appmeasurement_capture_appends_only_bss_requests():
    seen = {}
    beacons = []

    class FakePage:
        def on(self, event_name, handler) -> None:
            seen[event_name] = handler

    class FakeRequest:
        def __init__(self, url: str, post_data=None) -> None:
            self.url = url
            self.post_data = post_data

    attach_appmeasurement_capture(FakePage(), beacons)

    seen["request"](FakeRequest("https://example.test/api"))
    seen["request"](
        FakeRequest(
            "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0",
            "pageName=home",
        )
    )

    assert beacons == [{"rsid": "my-rsid", "pageName": "home"}]


def test_slice_appmeasurement_beacons_returns_copy():
    beacons = [{"pageName": "a"}, {"pageName": "b"}, {"pageName": "c"}]

    sliced = slice_appmeasurement_beacons(beacons, 1)

    assert sliced == [{"pageName": "b"}, {"pageName": "c"}]
    assert sliced is not beacons


def test_wait_for_appmeasurement_ready_prefers_beacon():
    class FakePage:
        def __init__(self) -> None:
            self.wait_calls = []

        def evaluate(self, _script: str) -> bool:
            return False

        def wait_for_timeout(self, wait_ms: int) -> None:
            self.wait_calls.append(wait_ms)
            if wait_ms == 1000:
                beacons.append({"pageName": "home"})

    beacons: list[dict[str, str]] = []
    page = FakePage()

    result = wait_for_appmeasurement_ready(
        page,
        beacons,
        timeout_ms=3000,
        poll_ms=1000,
        settle_ms=2000,
    )

    assert result == {"status": "beacon", "elapsedMs": 1000}
    assert page.wait_calls == [1000, 2000]


def test_wait_for_appmeasurement_ready_accepts_satellite_runtime():
    class FakePage:
        def __init__(self) -> None:
            self.wait_calls = []
            self.calls = 0

        def evaluate(self, _script: str) -> bool:
            self.calls += 1
            return self.calls >= 2

        def wait_for_timeout(self, wait_ms: int) -> None:
            self.wait_calls.append(wait_ms)

    page = FakePage()

    result = wait_for_appmeasurement_ready(
        page,
        [],
        timeout_ms=3000,
        poll_ms=1000,
        settle_ms=1500,
    )

    assert result == {"status": "satellite", "elapsedMs": 1000}
    assert page.wait_calls == [1000, 1500]


def test_wait_for_appmeasurement_ready_reports_timeout():
    class FakePage:
        def __init__(self) -> None:
            self.wait_calls = []

        def evaluate(self, _script: str) -> bool:
            return False

        def wait_for_timeout(self, wait_ms: int) -> None:
            self.wait_calls.append(wait_ms)

    page = FakePage()

    result = wait_for_appmeasurement_ready(
        page,
        [],
        timeout_ms=2500,
        poll_ms=1000,
        settle_ms=500,
    )

    assert result == {"status": "timeout", "elapsedMs": 2500}
    assert page.wait_calls == [1000, 1000, 1000]


def test_appmeasurement_capture_wraps_checkpoint_and_ready_helpers():
    capture = AppMeasurementCapture([{"pageName": "home"}])

    class FakePage:
        def __init__(self) -> None:
            self.handlers = {}
            self.wait_calls = []

        def on(self, event_name, handler) -> None:
            self.handlers[event_name] = handler

        def evaluate(self, _script: str) -> bool:
            return False

        def wait_for_timeout(self, wait_ms: int) -> None:
            self.wait_calls.append(wait_ms)

    page = FakePage()
    start_index = capture.checkpoint()
    capture.beacons.append({"pageName": "detail"})

    assert capture.since(start_index) == [{"pageName": "detail"}]
    assert capture.snapshot() == [{"pageName": "home"}, {"pageName": "detail"}]

    capture.attach(page)
    page.handlers["request"](
        type(
            "Req",
            (),
            {
                "url": "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0",
                "post_data": "pageName=checkout",
            },
        )()
    )
    assert capture.beacons[-1] == {"rsid": "my-rsid", "pageName": "checkout"}

    ready = capture.wait_until_ready(page, timeout_ms=1000, poll_ms=500, settle_ms=250)
    assert ready == {"status": "beacon", "elapsedMs": 0}
    assert page.wait_calls == [250]

    capture.clear()
    assert capture.beacons == []


def test_appmeasurement_capture_supports_custom_request_parser():
    capture = AppMeasurementCapture(
        parser=lambda request: {"url": request.url, "kind": "custom"}
        if "b/ss/" in request.url
        else None
    )

    class FakePage:
        def __init__(self) -> None:
            self.handlers = {}

        def on(self, event_name, handler) -> None:
            self.handlers[event_name] = handler

    page = FakePage()
    capture.attach(page)
    page.handlers["request"](type("Req", (), {"url": "https://example.test/api"})())
    page.handlers["request"](
        type("Req", (), {"url": "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0"})()
    )

    assert capture.beacons == [
        {
            "url": "https://example.sc.omtrdc.net/b/ss/my-rsid/1/JS-2.27.0",
            "kind": "custom",
        }
    ]


def test_appmeasurement_capture_collect_after_returns_incremental_beacons():
    capture = AppMeasurementCapture([{"pageName": "start"}])

    class FakePage:
        def __init__(self) -> None:
            self.wait_calls = []

        def wait_for_timeout(self, wait_ms: int) -> None:
            self.wait_calls.append(wait_ms)

    page = FakePage()

    def _action() -> str:
        capture.beacons.append({"pageName": "after"})
        return "ok"

    result, beacons = capture.collect_after(_action, page=page, wait_ms=300)

    assert result == "ok"
    assert beacons == [{"pageName": "after"}]
    assert page.wait_calls == [300]


def test_execute_appmeasurement_scenario_runs_declarative_steps():
    capture = AppMeasurementCapture()

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://example.test/start"
            self.wait_calls = []
            self.click_calls = []
            self.go_back_calls = 0

        def goto(self, url, **_kwargs):
            self.url = url
            capture.beacons.append({"pageName": f"goto:{url}"})
            return url

        def click(self, selector, force=False):
            self.click_calls.append((selector, force))
            capture.beacons.append({"pageName": f"click:{selector}"})
            return selector

        def go_back(self, **_kwargs):
            self.go_back_calls += 1
            capture.beacons.append({"pageName": "back"})
            return "back"

        def wait_for_timeout(self, wait_ms: int) -> None:
            self.wait_calls.append(wait_ms)

    page = FakePage()

    steps = execute_appmeasurement_scenario(
        page,
        capture,
        [
            {"name": "load", "action": "goto", "url": "https://example.test/home", "waitMs": 200},
            {"name": "cta", "action": "click", "selector": "#cta", "force": True, "waitMs": 300},
            {"name": "back", "action": "goBack"},
            {"name": "custom", "action": "callback", "callback": lambda: capture.beacons.append({"pageName": "custom"})},
        ],
    )

    assert [step["name"] for step in steps] == ["load", "cta", "back", "custom"]
    assert steps[0]["beacons"] == [{"pageName": "goto:https://example.test/home"}]
    assert steps[1]["beacons"] == [{"pageName": "click:#cta"}]
    assert steps[2]["beacons"] == [{"pageName": "back"}]
    assert steps[3]["beacons"] == [{"pageName": "custom"}]
    assert page.wait_calls == [200, 300]
    assert page.click_calls == [("#cta", True)]
    assert page.go_back_calls == 1


def test_run_aa_validation_supports_setup_before_steps_and_runtime(monkeypatch):
    import megaton_lib.validation.adobe_analytics as mod

    seen = []
    run_page_kwargs = {}

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://example.test/start"
            self.handlers = {}

        def on(self, event_name, handler) -> None:
            self.handlers[event_name] = handler

        def goto(self, url, **kwargs) -> None:
            self.url = url

        def wait_for_timeout(self, _wait_ms) -> None:
            return None

    def fake_run_page(url, **kwargs):
        run_page_kwargs["url"] = url
        run_page_kwargs.update(kwargs)
        page = FakePage()
        return kwargs["callback"](page)

    monkeypatch.setattr(mod, "run_page", fake_run_page)
    monkeypatch.setattr(
        mod,
        "capture_satellite_info",
        lambda page: {"hasSatellite": True, "buildDate": "2026-04-10T00:00:00Z"},
    )

    results = mod.run_aa_validation(
        {
            "name": "test-aa",
            "url": "https://example.test/start",
            "steps": [{"action": "goto", "url": "https://example.test/final", "waitMs": 0}],
            "waitSeconds": 0,
            "ignoreHttpsErrors": True,
            "storageState": {"cookies": []},
            "viewport": {"width": 1280, "height": 720},
            "pageSetup": lambda page: seen.append(("setup", page.url)),
            "bootstrapPage": lambda page: seen.append(("before", page.url)),
            "captureRuntime": lambda page: {"href": page.url},
        }
    )

    assert seen == [
        ("setup", "https://example.test/start"),
        ("before", "https://example.test/start"),
    ]
    assert run_page_kwargs["url"] == "https://example.test/final"
    assert run_page_kwargs["ignore_https_errors"] is True
    assert run_page_kwargs["storage_state"] == {"cookies": []}
    assert run_page_kwargs["viewport"] == {"width": 1280, "height": 720}
    assert results["url"] == "https://example.test/final"
    assert results["runtime"] == {"href": "https://example.test/final"}
    assert results["satellite"] == {
        "hasSatellite": True,
        "buildDate": "2026-04-10T00:00:00Z",
    }


def test_run_aa_validation_keeps_backward_compatible_hook_names(monkeypatch):
    import megaton_lib.validation.adobe_analytics as mod

    seen = []

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://example.test/start"

        def on(self, _event_name, _handler) -> None:
            return None

        def goto(self, url, **kwargs) -> None:
            self.url = url

        def wait_for_timeout(self, _wait_ms) -> None:
            return None

    monkeypatch.setattr(mod, "run_page", lambda url, **kwargs: kwargs["callback"](FakePage()))
    monkeypatch.setattr(mod, "capture_satellite_info", lambda page: {"hasSatellite": False})

    results = mod.run_aa_validation(
        {
            "name": "legacy-hooks",
            "url": "https://example.test/start",
            "steps": [{"action": "goto", "url": "https://example.test/final", "waitMs": 0}],
            "waitSeconds": 0,
            "beforeSteps": lambda page: seen.append(("before", page.url)),
            "runtimeSnapshot": lambda page: {"href": page.url},
        }
    )

    assert seen == [("before", "https://example.test/start")]
    assert results["runtime"] == {"href": "https://example.test/final"}
