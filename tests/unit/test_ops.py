import json

import httpx2
import pytest
from helpers import FakeConnector, call

pytestmark = pytest.mark.anyio

TYPES = [{"code": "APPLICATION_LOGIN", "description": "Login"}, {"code": "TRANSFER_FAILED", "description": "Failed"}]
EMPTY_PAGE = {
    "response": {"data": {"content": [], "page": {"size": 5, "totalElements": 0, "totalPages": 0, "number": 2}}}
}


def ok(body):
    return lambda _: httpx2.Response(200, json=body)


async def test_dashboard_and_audit_parameters():
    fake = FakeConnector(
        {
            ("GET", "/api/v1/dashboard/events"): ok({"totalCount": 3}),
            ("GET", "/api/v1/audit/types"): ok(TYPES),
            ("GET", "/api/v1/audit"): ok(EMPTY_PAGE),
            ("GET", "/api/v1/audit/6aeb"): ok({"id": "6aeb"}),
        }
    )
    window = {"from_": "2026-11-01T00:00:00Z", "to": "2026-11-04T00:00:00Z", "bucket": "day"}

    dashboard = await call(fake, "get_dashboard", view="events", **window, connector="a")
    events = await call(
        fake, "list_audit_events", event_type=["APPLICATION_LOGIN", "TRANSFER_FAILED"], page=2, size=5, connector="a"
    )
    one = await call(fake, "get_audit_event", id="6aeb", connector="a")

    assert not (dashboard.is_error or events.is_error or one.is_error)
    [metrics] = fake.calls("GET", "/api/v1/dashboard/events")
    assert dict(metrics.url.params) == {"from": "2026-11-01T00:00:00Z", "to": "2026-11-04T00:00:00Z", "bucket": "day"}
    [listed] = fake.calls("GET", "/api/v1/audit")
    assert listed.url.params.multi_items() == [
        ("page", "2"),
        ("size", "5"),
        ("eventType", "APPLICATION_LOGIN"),
        ("eventType", "TRANSFER_FAILED"),
    ]
    assert events.structured_content == {"items": [], "page": EMPTY_PAGE["response"]["data"]["page"]}
    assert json.loads(one.content[0].text) == {"id": "6aeb"}


async def test_dashboard_without_a_window_sends_no_parameters():
    fake = FakeConnector({("GET", "/api/v1/dashboard/summary"): ok({"negotiations": {}})})

    await call(fake, "get_dashboard", connector="a")

    assert dict(fake.requests[0].url.params) == {}


@pytest.mark.parametrize(
    "bad",
    [
        {"view": "runtime", "bucket": "hour"},
        {"view": "transfers", "from_": "2026-11-01T00:00:00Z"},
        {"view": "negotiations", "to": "2026-11-04T00:00:00Z"},
        {"bucket": "week"},
        {"view": "audit"},
    ],
)
async def test_dashboard_refuses_what_the_view_does_not_take(bad):
    fake = FakeConnector({})

    result = await call(fake, "get_dashboard", **bad, connector="a")

    assert result.is_error
    assert fake.requests == []


async def test_an_unknown_audit_code_is_refused_instead_of_matching_nothing():
    fake = FakeConnector({("GET", "/api/v1/audit/types"): ok(TYPES)})

    result = await call(fake, "list_audit_events", event_type=["Login"], connector="a")

    assert result.is_error
    assert "unknown event type codes ['Login']" in result.content[0].text
    assert "trueconnector://a/audit/types" in result.content[0].text
    assert [r.url.path for r in fake.requests] == ["/api/v1/audit/types"]  # the list itself is never called


async def test_audit_events_without_a_filter_skip_the_type_lookup():
    fake = FakeConnector({("GET", "/api/v1/audit"): ok(EMPTY_PAGE)})

    result = await call(fake, "list_audit_events", connector="a")

    assert not result.is_error, result.content
    assert dict(fake.requests[0].url.params) == {"page": "0", "size": "20"}
