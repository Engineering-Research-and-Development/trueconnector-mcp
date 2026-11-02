import json

import httpx2
import mcp
import pytest
from helpers import FIXTURES, TWO_CONNECTORS, FakeConnector, call

from tc_mcp.server import build_server

pytestmark = pytest.mark.anyio

PAGED = FIXTURES["paged"]["body"]["response"]["data"]
MINE = {k: v for k, v in PAGED["content"][0].items() if k != "links"}  # a consumer-side record on a


def page_of(*records):
    data = {
        "content": list(records),
        "page": {"size": 100, "totalElements": len(records), "totalPages": 1, "number": 0},
    }
    return lambda _: httpx2.Response(200, json={"response": {"success": True, "data": data}})


def one(record):
    return lambda _: httpx2.Response(200, json={"success": True, "data": record})


async def test_list_negotiations_maps_filters_to_api_fields():
    fake = FakeConnector({("GET", "/api/v1/negotiations"): "paged"})

    result = await call(
        fake,
        "list_negotiations",
        state="REQUESTED",
        role="provider",
        consumer_pid="urn:c",
        provider_pid="urn:p",
        assigner="urn:x",
        page=1,
        size=50,
        connector="a",
    )

    assert not result.is_error, result.content
    assert dict(fake.requests[0].url.params) == {
        "page": "1",
        "size": "50",
        "state": "REQUESTED",
        "role": "provider",
        "consumerPid": "urn:c",
        "providerPid": "urn:p",
        "assigner": "urn:x",
    }
    assert result.structured_content == {
        "items": [{k: v for k, v in item.items() if k != "links"} for item in PAGED["content"]],
        "page": PAGED["page"],
    }


async def test_list_negotiations_sends_only_paging_without_filters():
    fake = FakeConnector({("GET", "/api/v1/negotiations"): "paged"})

    await call(fake, "list_negotiations", connector="a")

    assert dict(fake.requests[0].url.params) == {"page": "0", "size": "20"}


@pytest.mark.parametrize(
    "bad", [{"state": "requested"}, {"role": "PROVIDER"}, {"size": 101}, {"size": 0}, {"page": -1}]
)
async def test_list_negotiations_rejects_bad_filters_before_sending(bad):
    fake = FakeConnector({})

    result = await call(fake, "list_negotiations", connector="a", **bad)

    assert result.is_error
    assert fake.requests == []


async def test_find_counterpart_skips_same_role_records():
    same_role = {**MINE, "@id": "urn:uuid:same-role-on-b"}
    provider = {**MINE, "@id": "urn:uuid:provider-on-b", "role": "provider"}
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{MINE['@id']}"): one(MINE),
            ("GET", "/api/v1/negotiations"): page_of(same_role, provider),
        }
    )

    result = await call(fake, "find_counterpart_negotiation", id=MINE["@id"], peer_connector="b", connector="a")

    assert not result.is_error, result.content
    assert json.loads(result.content[0].text) == provider
    looked_up, listed = fake.requests
    assert (looked_up.url.host, listed.url.host) == ("tc-a", "tc-b")  # read here, searched on the peer
    assert dict(listed.url.params) == {"page": "0", "size": "100", "consumerPid": MINE["consumerPid"]}


async def test_find_counterpart_reports_a_missing_counterpart():
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{MINE['@id']}"): one(MINE),
            ("GET", "/api/v1/negotiations"): page_of({**MINE, "@id": "urn:uuid:same-role-on-b"}),
        }
    )

    result = await call(fake, "find_counterpart_negotiation", id=MINE["@id"], peer_connector="b", connector="a")

    assert result.is_error
    assert f"b has no counterpart for consumerPid {MINE['consumerPid']} (role ≠ consumer)" in result.content[0].text


async def test_negotiation_tools_describe_the_state_machine_and_list_has_an_output_schema():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}

    for name in ("list_negotiations", "get_negotiation", "find_counterpart_negotiation"):
        assert "OFFERED -> REQUESTED | ACCEPTED | TERMINATED" in tools[name].description, name
    schema = tools["list_negotiations"].output_schema
    assert schema is not None and set(schema["properties"]) == {"items", "page"}
    state = tools["list_negotiations"].input_schema["properties"]["state"]
    assert "FINALIZED" in json.dumps(state)


OFFER = {"@id": "urn:uuid:offer", "target": "urn:uuid:dataset", "assigner": "urn:provider", "permission": []}
AGREEMENT_PATH = "/api/v1/agreements/urn:uuid:agreement/enforce"
CHECKS = [
    ("validate_offer", "/api/v1/offers/validate", {"offer": OFFER}, "valid"),
    ("enforce_agreement", AGREEMENT_PATH, {"agreement_id": "urn:uuid:agreement"}, "allowed"),
]


def answer(status, message, data=None):
    body = {"success": status == 200, "message": message, "data": data}
    return lambda _: httpx2.Response(status, json=body)


@pytest.mark.parametrize(("tool", "path", "args", "verdict"), CHECKS)
@pytest.mark.parametrize(
    ("status", "message", "expected"),
    [(200, "ok", True), (400, "Offer not valid", False), (400, "Agreement with id'x' evaluated as invalid", False)],
)
async def test_validate_and_enforce_map_rejections_to_false(tool, path, args, verdict, status, message, expected):
    fake = FakeConnector({("POST", path): answer(status, message, data=message if status == 200 else None)})

    result = await call(fake, tool, connector="b", **args)

    assert not result.is_error, result.content
    assert result.structured_content == {verdict: expected, "message": message}


@pytest.mark.parametrize(("tool", "path", "args", "verdict"), CHECKS)
@pytest.mark.parametrize(
    ("status", "message", "error"),
    [(500, "NullPointerException", "internal error"), (400, "Agreement with Id x not found.", "not found")],
)
async def test_validate_and_enforce_keep_failures_as_errors(tool, path, args, verdict, status, message, error):
    fake = FakeConnector({("POST", path): answer(status, message)})

    result = await call(fake, tool, connector="b", **args)

    assert result.is_error
    assert error in result.content[0].text


async def test_validate_sends_the_offer_and_enforce_an_empty_body():
    ok = answer(200, "ok", data="ok")
    fake = FakeConnector({("POST", "/api/v1/offers/validate"): ok, ("POST", AGREEMENT_PATH): ok})

    await call(fake, "validate_offer", offer=OFFER, connector="b")
    await call(fake, "enforce_agreement", agreement_id="urn:uuid:agreement", connector="b")

    validated, enforced = fake.requests
    assert json.loads(validated.content) == OFFER
    assert enforced.content == b"{}"


async def test_validate_and_enforce_have_output_schemas():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}

    assert set(tools["validate_offer"].output_schema["properties"]) == {"valid", "message"}
    assert set(tools["enforce_agreement"].output_schema["properties"]) == {"allowed", "message"}
