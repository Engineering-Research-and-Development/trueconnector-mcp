import json

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_dashboard_views(call, mcp_client):
    summary = await call(
        "get_dashboard", from_="2026-01-01T00:00:00Z", to="2099-01-01T00:00:00Z", bucket="day", connector="a"
    )
    assert {"negotiations", "transfers"} <= summary.keys()
    assert summary["negotiations"]["totalCount"] >= 1
    runtime = await call("get_dashboard", view="runtime", connector="a")
    assert runtime["uptimeMilliseconds"] > 0

    bad = await mcp_client.call_tool("get_dashboard", {"from_": "yesterday", "connector": "a"})
    assert bad.is_error
    assert "Invalid timestamp: yesterday" in bad.content[0].text  # TrueConnector's own message


async def test_audit_events_are_filtered_by_code_and_read_one_by_one(call, mcp_client):
    types = json.loads((await mcp_client.read_resource("trueconnector://a/audit/types")).contents[0].text)
    login = next(t["description"] for t in types if t["code"] == "APPLICATION_LOGIN")

    events = await call("list_audit_events", event_type=["APPLICATION_LOGIN"], size=5, connector="a")

    assert events["items"], "every test session logs in"
    assert {e["eventType"] for e in events["items"]} == {login}  # shown by description, filtered by code
    first = events["items"][0]
    assert await call("get_audit_event", id=first["id"], connector="a") == first
    wrong = await mcp_client.call_tool("list_audit_events", {"event_type": [login], "connector": "a"})
    assert wrong.is_error and "unknown event type codes" in wrong.content[0].text
