from __future__ import annotations

from megaton_lib.audit.providers.analytics.dw.scheduler import (
    build_cloned_request_body,
    collect_scheduled_requests,
    find_template_requests,
    resolve_template_request,
    summarize_template_detail,
)


class _DummyClient:
    def __init__(
        self,
        list_payload=None,
        list_payloads=None,
        detail_payloads=None,
        list_handler=None,
    ):
        self._list_payload = list_payload or {"scheduledRequests": []}
        self._list_payloads = list(list_payloads or [])
        self._detail_payloads = detail_payloads or {}
        self._list_handler = list_handler
        self.list_calls = []

    def list_scheduled_requests(self, **kwargs):
        self.list_kwargs = kwargs
        self.list_calls.append(kwargs)
        if self._list_handler is not None:
            return self._list_handler(**kwargs)
        if self._list_payloads:
            return self._list_payloads.pop(0)
        return self._list_payload

    def get_scheduled_request(self, scheduled_request_uuid):
        return self._detail_payloads[scheduled_request_uuid]


def test_build_cloned_request_body_whitelists_template_fields():
    template = {
        "metadata": {"scheduledRequestUUID": "old"},
        "schedule": {
            "scheduleAt": "2026-04-01T00:00:00Z",
            "periodSettings": {"frequency": "daily", "every": 1},
            "cancelSettings": {"cancelMethod": "afterOccurrences", "endAfterNumOccurrences": 1},
        },
        "request": {
            "name": "template",
            "rsid": "wacoal-all",
            "sharing": {"shareWithOtherUsers": True},
            "outputFile": {
                "fileFormat": "csv",
                "outputFileBasename": "Template",
                "compressionFormat": "default",
            },
            "reportParameters": {
                "dimensionList": [{"id": "variables/evar31"}],
                "metricList": [{"id": "metrics/orders"}],
                "reportRange": {"preset": "Last month", "startDateTime": None, "endDateTime": None},
            },
        },
        "delivery": {"exportLocationUUID": "loc-1", "legacyFTP": None},
    }

    body = build_cloned_request_body(
        template_detail=template,
        name="step 20240401-20260331",
        schedule_at="2026-04-06T03:00:00+09:00",
        report_range={
            "preset": "dateRange",
            "dateFrom": "2024-04-01",
            "dateTo": "2026-03-31",
        },
        output_file_basename="WWS-ID-20240401-20260331",
    )

    assert body == {
        "schedule": {
            "scheduleAt": "2026-04-06T03:00:00+09:00",
            "periodSettings": {"frequency": "daily", "every": 1},
            "cancelSettings": {"cancelMethod": "afterOccurrences", "endAfterNumOccurrences": 1},
        },
        "request": {
            "name": "step 20240401-20260331",
            "sharing": {"shareWithOtherUsers": True},
            "outputFile": {
                "fileFormat": "csv",
                "outputFileBasename": "WWS-ID-20240401-20260331",
                "compressionFormat": "default",
            },
            "reportParameters": {
                "dimensionList": [{"id": "variables/evar31"}],
                "metricList": [{"id": "metrics/orders"}],
                "reportRange": {
                    "preset": "dateRange",
                    "startDateTime": "2024-04-01T00:00:00Z",
                    "endDateTime": "2026-03-31T23:59:59Z",
                },
            },
            "rsid": "wacoal-all",
        },
        "delivery": {"exportLocationUUID": "loc-1"},
    }


def test_find_template_requests_filters_summary_fields():
    client = _DummyClient(
        list_payload={
            "scheduledRequests": [
                {
                    "request": {"name": "tmpl_step_id_detail_gcs", "rsid": "wacoal-all"},
                    "metadata": {
                        "scheduledRequestUUID": "uuid-1",
                        "status": "Completed",
                        "createdDate": "2026-01-01T00:00:00Z",
                        "updatedDate": "2026-03-01T00:00:00Z",
                        "ownerInfo": {"login": "owner@example.com"},
                    },
                },
                {
                    "request": {"name": "other", "rsid": "wacoal-all"},
                    "metadata": {
                        "scheduledRequestUUID": "uuid-2",
                        "status": "Error",
                        "createdDate": "2026-01-01T00:00:00Z",
                        "updatedDate": "2026-02-01T00:00:00Z",
                        "ownerInfo": {"login": "owner@example.com"},
                    },
                },
            ]
        }
    )

    found = find_template_requests(
        client,
        rsid="wacoal-all",
        name_contains="tmpl_step_",
        owner_login="owner@example.com",
        status=["Completed"],
    )

    assert [item["scheduled_request_uuid"] for item in found] == ["uuid-1"]
    assert client.list_kwargs["rsid"] == "wacoal-all"


def test_collect_scheduled_requests_uses_updated_before_window_when_page_is_unreliable():
    client = _DummyClient(
        list_payloads=[
            {
                "total": 3,
                "scheduledRequests": [
                    {
                        "request": {"name": "latest", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-3",
                            "updatedDate": "2026-03-03T00:00:00Z",
                        },
                    },
                    {
                        "request": {"name": "middle", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-2",
                            "updatedDate": "2026-03-02T00:00:00Z",
                        },
                    },
                ],
            },
            {
                "total": 1,
                "scheduledRequests": [
                    {
                        "request": {"name": "oldest", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-1",
                            "updatedDate": "2026-03-01T00:00:00Z",
                        },
                    },
                ],
            },
        ]
    )

    collected = collect_scheduled_requests(client, rsid="wacoal-all", limit=2)

    assert [item["metadata"]["scheduledRequestUUID"] for item in collected["scheduledRequests"]] == [
        "uuid-3",
        "uuid-2",
        "uuid-1",
    ]
    assert client.list_calls[1]["updated_before"] == "2026-03-01T23:59:59Z"


def test_collect_scheduled_requests_fetches_same_timestamp_boundary_bucket():
    boundary_dt = "2026-03-01T00:00:00Z"

    def _list_handler(**kwargs):
        if kwargs.get("updated_after") == boundary_dt and kwargs.get("updated_before") == boundary_dt:
            return {
                "total": 2,
                "scheduledRequests": [
                    {
                        "request": {"name": "boundary-a", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-boundary-a",
                            "updatedDate": boundary_dt,
                        },
                    },
                    {
                        "request": {"name": "boundary-b", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-boundary-b",
                            "updatedDate": boundary_dt,
                        },
                    },
                ],
            }
        if kwargs.get("updated_before") == "2026-02-28T23:59:59Z":
            return {
                "total": 1,
                "scheduledRequests": [
                    {
                        "request": {"name": "older", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-older",
                            "updatedDate": "2026-02-28T00:00:00Z",
                        },
                    }
                ],
            }
        return {
            "total": 102,
            "scheduledRequests": [
                *[
                    {
                        "request": {"name": f"latest-{index}", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": f"uuid-latest-{index}",
                            "updatedDate": f"2026-03-{99 - index:02d}T00:00:00Z",
                        },
                    }
                    for index in range(99)
                ],
                {
                    "request": {"name": "boundary-a", "rsid": "wacoal-all"},
                    "metadata": {
                        "scheduledRequestUUID": "uuid-boundary-a",
                        "updatedDate": boundary_dt,
                    },
                },
            ],
        }

    client = _DummyClient(list_handler=_list_handler)

    collected = collect_scheduled_requests(client, rsid="wacoal-all")

    assert collected["total"] == 102
    assert [item["metadata"]["scheduledRequestUUID"] for item in collected["scheduledRequests"][:2]] == [
        "uuid-latest-0",
        "uuid-latest-1",
    ]
    assert {
        item["metadata"]["scheduledRequestUUID"] for item in collected["scheduledRequests"]
    } >= {"uuid-boundary-a", "uuid-boundary-b", "uuid-older"}
    assert client.list_calls[1]["updated_after"] == boundary_dt
    assert client.list_calls[1]["updated_before"] == boundary_dt
    assert client.list_calls[2]["updated_before"] == "2026-02-28T23:59:59Z"


def test_collect_scheduled_requests_raises_when_same_timestamp_bucket_exceeds_limit():
    boundary_dt = "2026-03-01T00:00:00Z"

    def _list_handler(**kwargs):
        if kwargs.get("updated_after") == boundary_dt and kwargs.get("updated_before") == boundary_dt:
            return {
                "total": 101,
                "scheduledRequests": [
                    {
                        "request": {"name": f"boundary-{index}", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": f"uuid-boundary-{index}",
                            "updatedDate": boundary_dt,
                        },
                    }
                    for index in range(100)
                ],
            }
        return {
            "total": 101,
            "scheduledRequests": [
                *[
                    {
                        "request": {"name": f"latest-{index}", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": f"uuid-latest-{index}",
                            "updatedDate": f"2026-03-{99 - index:02d}T00:00:00Z",
                        },
                    }
                    for index in range(99)
                ],
                {
                    "request": {"name": "boundary-0", "rsid": "wacoal-all"},
                    "metadata": {
                        "scheduledRequestUUID": "uuid-boundary-0",
                        "updatedDate": boundary_dt,
                    },
                },
            ],
        }

    client = _DummyClient(list_handler=_list_handler)

    try:
        collect_scheduled_requests(client, rsid="wacoal-all")
    except RuntimeError as exc:
        assert "cannot be collected safely" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_find_template_requests_scans_multiple_windows_before_filtering():
    client = _DummyClient(
        list_payloads=[
            {
                "total": 2,
                "scheduledRequests": [
                    {
                        "request": {"name": "other", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-1",
                            "status": "Completed",
                            "updatedDate": "2026-03-02T00:00:00Z",
                            "ownerInfo": {"login": "owner@example.com"},
                        },
                    }
                ]
            },
            {
                "total": 1,
                "scheduledRequests": [
                    {
                        "request": {"name": "tmpl_step_id_detail_gcs", "rsid": "wacoal-all"},
                        "metadata": {
                            "scheduledRequestUUID": "uuid-2",
                            "status": "Completed",
                            "updatedDate": "2026-03-01T00:00:00Z",
                            "ownerInfo": {"login": "owner@example.com"},
                        },
                    }
                ]
            },
        ]
    )

    found = find_template_requests(
        client,
        rsid="wacoal-all",
        name_contains="tmpl_step_",
        owner_login="owner@example.com",
        status=["Completed"],
    )

    assert [item["scheduled_request_uuid"] for item in found] == ["uuid-2"]


def test_resolve_template_request_prefers_uuid_and_validates_rsid():
    detail = {
        "request": {"rsid": "wacoal-all"},
        "metadata": {"scheduledRequestUUID": "uuid-1", "updatedDate": "2026-03-01T00:00:00Z"},
        "delivery": {"exportLocationUUID": "loc-1"},
    }
    client = _DummyClient(detail_payloads={"uuid-1": detail})

    resolved = resolve_template_request(
        client,
        rsid="wacoal-all",
        scheduled_request_uuid="uuid-1",
    )

    assert resolved is detail


def test_resolve_template_request_filters_detail_fields():
    client = _DummyClient(
        list_payload={
            "scheduledRequests": [
                {
                    "request": {"name": "tmpl_step", "rsid": "wacoal-all"},
                    "metadata": {
                        "scheduledRequestUUID": "uuid-1",
                        "updatedDate": "2026-03-01T00:00:00Z",
                    },
                },
                {
                    "request": {"name": "tmpl_step", "rsid": "wacoal-all"},
                    "metadata": {
                        "scheduledRequestUUID": "uuid-2",
                        "updatedDate": "2026-04-01T00:00:00Z",
                    },
                },
            ]
        },
        detail_payloads={
            "uuid-1": {
                "request": {
                    "rsid": "wacoal-all",
                    "outputFile": {"outputFileBasename": "A"},
                    "reportParameters": {
                        "segmentList": [{"id": "seg-a"}],
                    },
                },
                "metadata": {"scheduledRequestUUID": "uuid-1", "updatedDate": "2026-03-01T00:00:00Z"},
                "delivery": {"exportLocationUUID": "loc-1"},
            },
            "uuid-2": {
                "request": {
                    "rsid": "wacoal-all",
                    "outputFile": {"outputFileBasename": "B"},
                    "reportParameters": {
                        "segmentList": [{"id": "seg-b"}],
                    },
                },
                "metadata": {"scheduledRequestUUID": "uuid-2", "updatedDate": "2026-04-01T00:00:00Z"},
                "delivery": {"exportLocationUUID": "loc-2"},
            },
        },
    )

    resolved = resolve_template_request(
        client,
        rsid="wacoal-all",
        name_contains="tmpl_step",
        output_file_basename="B",
        segment_id="seg-b",
    )

    assert resolved["metadata"]["scheduledRequestUUID"] == "uuid-2"


def test_summarize_template_detail_extracts_disambiguating_fields():
    detail = {
        "metadata": {
            "scheduledRequestUUID": "uuid-1",
            "status": "Completed",
            "createdDate": "2026-03-10T07:09:07Z",
            "updatedDate": "2026-03-31T15:23:44Z",
            "ownerInfo": {"login": "owner@example.com"},
        },
        "schedule": {
            "scheduleAt": None,
            "periodSettings": {"frequency": "runOnceSetPeriod"},
        },
        "request": {
            "name": "tmpl_step_id_detail_gcs",
            "rsid": "wacoal-all",
            "outputFile": {
                "outputFileBasename": "WWS-ID-20240401-20260331",
                "fileFormat": "csv",
                "compressionFormat": "default",
            },
            "reportParameters": {
                "dimensionList": [{"id": "variables/evar31"}],
                "metricList": [{"id": "metrics/orders"}, {"id": "metrics/revenue"}],
                "segmentList": [{"id": "seg-a"}],
                "reportRange": {
                    "preset": None,
                    "startDateTime": "2024-04-01T00:00:00Z",
                    "endDateTime": "2026-03-31T23:59:59Z",
                },
                "dateGranularity": "none",
                "numberOfRowsInTable": None,
            },
        },
        "delivery": {"exportLocationUUID": "loc-1"},
    }

    summary = summarize_template_detail(detail)

    assert summary == {
        "scheduled_request_uuid": "uuid-1",
        "name": "tmpl_step_id_detail_gcs",
        "rsid": "wacoal-all",
        "owner_login": "owner@example.com",
        "status": "Completed",
        "created_date": "2026-03-10T07:09:07Z",
        "updated_date": "2026-03-31T15:23:44Z",
        "schedule_at": "",
        "schedule_frequency": "runOnceSetPeriod",
        "output_file_basename": "WWS-ID-20240401-20260331",
        "file_format": "csv",
        "compression_format": "default",
        "export_location_uuid": "loc-1",
        "dimension_ids": ["variables/evar31"],
        "metric_ids": ["metrics/orders", "metrics/revenue"],
        "segment_ids": ["seg-a"],
        "report_range": {
            "preset": None,
            "startDateTime": "2024-04-01T00:00:00Z",
            "endDateTime": "2026-03-31T23:59:59Z",
        },
        "date_granularity": "none",
        "number_of_rows_in_table": None,
    }
