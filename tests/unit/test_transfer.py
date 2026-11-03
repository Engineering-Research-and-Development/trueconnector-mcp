import json

import httpx2
import mcp
import pytest
from helpers import TWO_CONNECTORS, FakeConnector, call

from tc_mcp.server import build_server

pytestmark = pytest.mark.anyio

AGREEMENT = "urn:uuid:c87e6929-2f19-4b49-93e8-977a98fabbb1"
# A live consumer-side transfer on connector A, right after its negotiation was finalized.
INITIALIZED = {
    "@id": "urn:uuid:720f3f3a-d5de-43b0-989d-80d42fae8665",
    "consumerPid": "urn:uuid:9a17fe21-dbd6-48de-ad33-5ed91251c42e",
    "providerPid": "urn:uuid:703af16b-598d-44b7-b12f-3c82207c610e",
    "agreementId": AGREEMENT,
    "callbackAddress": "http://connector-b:8080/connector-b",
    "datasetId": "urn:uuid:fdc45798-a222-4955-8baf-ab7fd66ac4d5",
    "role": "consumer",
    "state": "INITIALIZED",
    "format": None,
    "dataAddress": None,
    "downloaded": False,
    "downloadInProgress": False,
}
TID = INITIALIZED["@id"]
PROVIDER_SIDE = {**INITIALIZED, "@id": "urn:uuid:ca52156b-4876-4c75-a2ec-abf87f3264c7", "role": "provider"}


def page_of(*records):
    data = {
        "content": list(records),
        "page": {"size": 100, "totalElements": len(records), "totalPages": 1, "number": 0},
    }
    return lambda _: httpx2.Response(200, json={"response": {"success": True, "data": data}})


def one(record):
    return lambda _: httpx2.Response(200, json={"success": True, "data": record})


async def test_list_transfers_maps_filters_to_api_fields():
    fake = FakeConnector({("GET", "/api/v1/transfers"): page_of(INITIALIZED)})

    result = await call(
        fake,
        "list_transfers",
        state="INITIALIZED",
        role="consumer",
        agreement_id=AGREEMENT,
        dataset_id="urn:d",
        format="HttpData-PUSH",
        page=2,
        size=10,
        connector="a",
    )

    assert not result.is_error, result.content
    assert dict(fake.requests[0].url.params) == {
        "page": "2",
        "size": "10",
        "state": "INITIALIZED",
        "role": "consumer",
        "agreementId": AGREEMENT,
        "datasetId": "urn:d",
        "format": "HttpData-PUSH",
    }
    assert result.structured_content["items"] == [INITIALIZED]


@pytest.mark.parametrize("bad", [{"state": "started"}, {"role": "CONSUMER"}, {"format": "SFTP"}, {"size": 101}])
async def test_list_transfers_rejects_bad_filters_before_sending(bad):
    fake = FakeConnector({})

    result = await call(fake, "list_transfers", connector="a", **bad)

    assert result.is_error
    assert fake.requests == []


@pytest.mark.parametrize("role", [None, "consumer"])
async def test_find_transfer_for_agreement(role):
    fake = FakeConnector({("GET", "/api/v1/transfers"): page_of(INITIALIZED)})

    result = await call(fake, "find_transfer_for_agreement", agreement_id=AGREEMENT, role=role, connector="a")

    assert not result.is_error, result.content
    assert json.loads(result.content[0].text) == INITIALIZED
    expected = {"page": "0", "size": "100", "agreementId": AGREEMENT} | ({"role": role} if role else {})
    assert dict(fake.requests[0].url.params) == expected


@pytest.mark.parametrize(
    ("records", "role", "error"),
    [
        ((), None, f"connector 'a' has no transfer for agreement {AGREEMENT}: is its negotiation FINALIZED?"),
        ((), "provider", f"no transfer for agreement {AGREEMENT} with role provider"),
        ((INITIALIZED, PROVIDER_SIDE), None, f"connector 'a' has 2 transfers for agreement {AGREEMENT}: pass role"),
    ],
)
async def test_find_transfer_for_agreement_errors(records, role, error):
    fake = FakeConnector({("GET", "/api/v1/transfers"): page_of(*records)})

    result = await call(fake, "find_transfer_for_agreement", agreement_id=AGREEMENT, role=role, connector="a")

    assert result.is_error
    assert error in result.content[0].text


async def test_request_transfer_sends_only_the_process_and_format():
    requested = {**INITIALIZED, "state": "REQUESTED", "format": "HttpData-PULL"}
    fake = FakeConnector(
        {("GET", f"/api/v1/transfers/{TID}"): one(INITIALIZED), ("POST", "/api/v1/transfers"): one(requested)}
    )

    result = await call(fake, "request_transfer", transfer_process_id=TID, format="HttpData-PULL", connector="a")

    assert result.structured_content == {"already_done": False, "state": "REQUESTED", "transfer": requested}
    sent = fake.calls("POST", "/api/v1/transfers")[0]
    assert json.loads(sent.content) == {"transferProcessId": TID, "format": "HttpData-PULL"}  # no dataAddress


async def test_repeated_request_is_already_done():
    requested = {**INITIALIZED, "state": "REQUESTED", "format": "HttpData-PUSH"}
    already = {"success": False, "message": "State transition aborted, REQUESTED state can not transition to REQUESTED"}
    reads = iter([INITIALIZED | {"state": "REQUESTED"}, requested])
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/transfers/{TID}"): lambda _: httpx2.Response(
                200, json={"data": next(reads), "success": True}
            ),
            ("POST", "/api/v1/transfers"): lambda _: httpx2.Response(400, json=already),
        }
    )

    result = await call(fake, "request_transfer", transfer_process_id=TID, format="HttpData-PUSH", connector="a")

    assert result.structured_content == {"already_done": True, "state": "REQUESTED", "transfer": requested}


async def test_request_transfer_on_the_provider_record_is_refused_locally():
    fake = FakeConnector({("GET", f"/api/v1/transfers/{PROVIDER_SIDE['@id']}"): one(PROVIDER_SIDE)})

    result = await call(
        fake, "request_transfer", transfer_process_id=PROVIDER_SIDE["@id"], format="HttpData-PULL", connector="b"
    )

    assert result.is_error
    assert "request is a consumer action, but" in result.content[0].text
    assert [r.method for r in fake.requests] == ["GET"]


async def test_request_transfer_rejects_an_unknown_format_before_sending():
    fake = FakeConnector({})

    result = await call(fake, "request_transfer", transfer_process_id=TID, format="SFTP", connector="a")

    assert result.is_error
    assert fake.requests == []


async def test_transfer_tools_describe_the_state_machine():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}

    for name in ("list_transfers", "get_transfer", "find_transfer_for_agreement", "request_transfer"):
        assert "STARTED -> SUSPENDED | COMPLETED | TERMINATED" in tools[name].description, name
    assert set(tools["request_transfer"].output_schema["properties"]) == {"already_done", "state", "transfer"}
    assert set(tools["list_transfers"].output_schema["properties"]) == {"items", "page"}


ACTIONS = [("start", "STARTED"), ("complete", "COMPLETED"), ("suspend", "SUSPENDED"), ("terminate", "TERMINATED")]


@pytest.mark.parametrize(("action", "state"), ACTIONS)
async def test_lifecycle_transition_returns_the_record_it_gets_back(action, state):
    after = {**INITIALIZED, "state": state}
    fake = FakeConnector({("PUT", f"/api/v1/transfers/{TID}/{action}"): one(after)})

    result = await call(fake, f"{action}_transfer", transfer_process_id=TID, connector="a")

    assert result.structured_content == {"already_done": False, "state": state, "transfer": after}
    [sent] = fake.requests
    assert sent.content == b"{}"  # the endpoints answer 415 without a JSON body


@pytest.mark.parametrize(("action", "state"), ACTIONS)
async def test_repeated_transfer_transition_is_already_done(action, state):
    already = {"success": False, "message": f"State transition aborted, {state} state can not transition to {state}"}
    now = {**INITIALIZED, "state": state}
    fake = FakeConnector(
        {
            ("PUT", f"/api/v1/transfers/{TID}/{action}"): lambda _: httpx2.Response(400, json=already),
            ("GET", f"/api/v1/transfers/{TID}"): one(now),
        }
    )

    result = await call(fake, f"{action}_transfer", transfer_process_id=TID, connector="a")

    assert result.structured_content == {"already_done": True, "state": state, "transfer": now}


async def test_an_illegal_or_wrong_side_transfer_transition_stays_an_error():
    # Live: TrueConnector itself names the side when the wrong one calls start.
    wrong = {
        "success": False,
        "message": "State transition aborted, consumer can not transit from REQUESTED to STARTED",
    }
    fake = FakeConnector({("PUT", f"/api/v1/transfers/{TID}/start"): lambda _: httpx2.Response(400, json=wrong)})

    result = await call(fake, "start_transfer", transfer_process_id=TID, connector="a")

    assert result.is_error
    assert "consumer can not transit from REQUESTED to STARTED" in result.content[0].text


async def test_transfer_transitions_describe_the_state_machine_and_have_an_output_schema():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}

    for action, _ in ACTIONS:
        tool = tools[f"{action}_transfer"]
        assert "STARTED -> SUSPENDED | COMPLETED | TERMINATED" in tool.description, action
        assert "already_done" in tool.description, action
        assert set(tool.output_schema["properties"]) == {"already_done", "state", "transfer"}, action
