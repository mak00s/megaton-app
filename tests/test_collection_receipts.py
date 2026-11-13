"""Event delivery evidence must survive unrelated successes and failures."""
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

from megaton_lib.validation.collection_receipts import (
    CollectionReceiptError, CollectionReceipts, named_events,
)


def delivered(tracker, events, status=204, stream="test"):
    request = object()
    tracker.request(request, events, stream=stream)
    tracker.response(SimpleNamespace(request=request, status=status))
    return request


def test_ga_probe_success_cannot_hide_failed_pageview():
    tracker = CollectionReceipts()
    delivered(tracker, ["at_impression"])
    delivered(tracker, ["page_view"], status=500)
    with pytest.raises(CollectionReceiptError, match="http_error:page_view"):
        tracker.require(["page_view"], stream="test")


def test_karte_other_post_cannot_hide_failed_identify():
    tracker = CollectionReceipts()
    delivered(tracker, ["unrelated"])
    delivered(tracker, ["identify"], status=500)
    with pytest.raises(CollectionReceiptError, match="http_error:identify"):
        tracker.require(["identify"])


def test_missing_events_and_pending_responses_are_not_receipts():
    tracker = CollectionReceipts()
    delivered(tracker, [])
    assert not tracker.received(["identify"])
    tracker.request(object(), ["identify"])
    assert not tracker.received(["identify"])


def test_response_correlates_by_request_not_arrival_order():
    tracker = CollectionReceipts()
    pv, probe = object(), object()
    tracker.request(pv, ["page_view"])
    tracker.request(probe, ["at_impression"])
    tracker.response(SimpleNamespace(request=probe, status=204))
    assert not tracker.received(["page_view"])
    tracker.response(SimpleNamespace(request=pv, status=200))
    assert tracker.received(["page_view"])


def test_network_failure_and_failed_repeat_cannot_be_hidden():
    tracker = CollectionReceipts()
    delivered(tracker, ["page_view"])
    failed = delivered(tracker, ["page_view"])
    tracker.failure(failed)
    assert not tracker.received(["page_view"])


def test_batch_matches_all_required_events_but_not_another_stream():
    tracker = CollectionReceipts()
    delivered(tracker, ["identify", "attribute"], stream="test")
    assert tracker.received(["identify", "attribute"], stream="test")
    assert not tracker.received(["identify"], stream="other")
    assert not tracker.received(["identify", "buy"])


@pytest.mark.parametrize("payload", [
    '{"events":[{"event_name":"identify"},{"event_name":"attribute"}]}',
    '{"data":"{\\"event_name\\":\\"identify\\"}","type":"attribute"}',
    urlencode({"data": '[{"event_name":"identify"},{"event_name":"attribute"}]'}),
])
def test_json_and_form_payloads_only_export_known_event_names(payload):
    assert named_events(payload, {"identify", "attribute"}) == ["attribute", "identify"]


def test_unknown_payloads_are_not_inferred_and_payloads_are_not_saved():
    assert named_events("opaque-encoded-body", {"identify"}) == []
    tracker = CollectionReceipts()
    delivered(tracker, ["identify"])
    assert set(tracker.records[0]) == {"requestId", "events", "stream", "status", "failed"}


def test_no_expected_events_is_not_delivery_evidence():
    assert not CollectionReceipts().received([])


def test_mentioning_event_in_unrelated_payload_is_not_delivery_evidence():
    assert named_events('{"log":"identify","lastEvent":"attribute"}',
                        {"identify", "attribute"}) == []
