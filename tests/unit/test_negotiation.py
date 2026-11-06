import json

import httpx2
import mcp
import pytest
from helpers import B_PROTOCOL, FIXTURES, TWO_CONNECTORS, FakeConnector, call

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

    for name in (
        "list_negotiations",
        "get_negotiation",
        "find_counterpart_negotiation",
        "request_contract",
        "send_counteroffer",
    ):
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


CATALOG = FIXTURES["proxy_catalog"]["body"]["data"]
DATASET = CATALOG["dataset"][0]
CATALOG_OFFER = DATASET["hasPolicy"][0]
SECOND_OFFER = {**CATALOG_OFFER, "@id": "urn:uuid:second-offer"}


def catalog_with(*offers):
    catalog = {**CATALOG, "dataset": [{**DATASET, "hasPolicy": list(offers)}]}
    return one(catalog)


@pytest.mark.parametrize(
    ("offers", "offer_id", "chosen"),
    [((CATALOG_OFFER,), None, CATALOG_OFFER), ((CATALOG_OFFER, SECOND_OFFER), SECOND_OFFER["@id"], SECOND_OFFER)],
)
async def test_request_contract_builds_offer_from_catalog(offers, offer_id, chosen):
    fake = FakeConnector(
        {
            ("POST", "/api/v1/proxy/catalogs"): catalog_with(*offers),
            ("POST", "/api/v1/negotiations/request"): one(MINE),
        }
    )

    result = await call(fake, "request_contract", peer="b", dataset_id=DATASET["@id"], offer_id=offer_id, connector="a")

    assert not result.is_error, result.content
    assert json.loads(result.content[0].text) == MINE
    browsed, requested = fake.requests
    assert {browsed.url.host, requested.url.host} == {"tc-a"}  # the consumer does both
    assert json.loads(browsed.content) == {"Forward-To": B_PROTOCOL}
    assert json.loads(requested.content) == {
        "Forward-To": B_PROTOCOL,
        "offer": chosen | {"target": DATASET["@id"], "assigner": CATALOG["participantId"]},
    }


@pytest.mark.parametrize(
    ("offers", "args", "error"),
    [
        ((CATALOG_OFFER,), {"dataset_id": "urn:uuid:other"}, "dataset urn:uuid:other is not in the catalog of"),
        ((CATALOG_OFFER, SECOND_OFFER), {}, "has 2 offers: pass offer_id, one of"),
        ((), {}, "has 0 offers: pass offer_id"),
        ((CATALOG_OFFER,), {"offer_id": "urn:uuid:other"}, "offer urn:uuid:other is not one of dataset"),
    ],
)
async def test_request_contract_errors(offers, args, error):
    fake = FakeConnector({("POST", "/api/v1/proxy/catalogs"): catalog_with(*offers)})

    result = await call(fake, "request_contract", **({"peer": "b", "dataset_id": DATASET["@id"]} | args), connector="a")

    assert result.is_error
    assert error in result.content[0].text
    assert not fake.calls("POST", "/api/v1/negotiations/request")


COUNT_3 = {"action": "USE", "constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": "3"}]}
NEG_ID = MINE["@id"]
STORED = MINE["offer"]  # a live consumer-side offer: its own @id, the catalog offer as originalId, no assignee
CONSUMER_CALLBACK = "http://connector-a:8080/connector-a/consumer"


def record(role, assignee=None, callback=None):
    return {**MINE, "role": role, "callbackAddress": callback, "offer": {**STORED, "assignee": assignee}}


def countered(fake, action):
    return json.loads(fake.calls("PUT", f"/api/v1/negotiations/{NEG_ID}/{action}")[0].content)


@pytest.mark.parametrize(("role", "action"), [("provider", "offer"), ("consumer", "request")])
async def test_counteroffer_uses_original_offer_id_target_and_assignee(role, action):
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): one(record(role, assignee="urn:stored", callback="http://cb")),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/{action}"): one(MINE),
        }
    )

    result = await call(fake, "send_counteroffer", negotiation_id=NEG_ID, permissions=[COUNT_3], connector="b")

    assert not result.is_error, result.content
    assert countered(fake, action) == {
        "@id": STORED["originalId"],
        "target": STORED["target"],
        "assigner": STORED["assigner"],
        "assignee": "urn:stored",
        "permission": [COUNT_3],
    }


async def test_provider_counteroffer_falls_back_to_callback_address_as_assignee():
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): one(record("provider", callback=CONSUMER_CALLBACK)),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/offer"): one(MINE),
        }
    )

    await call(fake, "send_counteroffer", negotiation_id=NEG_ID, permissions=[COUNT_3], connector="a")

    assert countered(fake, "offer")["assignee"] == CONSUMER_CALLBACK


async def test_consumer_counteroffer_falls_back_to_own_consumer_callback_as_assignee():
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): one(record("consumer", callback="http://provider")),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/request"): one(MINE),
        }
    )

    await call(fake, "send_counteroffer", negotiation_id=NEG_ID, permissions=[COUNT_3], connector="b")

    assert countered(fake, "request")["assignee"] == f"{B_PROTOCOL}/consumer"


@pytest.mark.parametrize(
    ("role", "missing"),
    [("provider", "no callbackAddress"), ("consumer", "no protocol_url configured for connector 'a'")],
)
async def test_counteroffer_without_any_assignee_is_refused_locally(role, missing):
    # Connector a has no protocol_url; a provider record without callbackAddress has nothing to fall back to either.
    fake = FakeConnector({("GET", f"/api/v1/negotiations/{NEG_ID}"): one(record(role, callback=None))})

    result = await call(fake, "send_counteroffer", negotiation_id=NEG_ID, permissions=[COUNT_3], connector="a")

    assert result.is_error
    assert f"negotiation {NEG_ID} has no assignee to keep and {missing} to derive one from" in result.content[0].text
    assert [r.method for r in fake.requests] == ["GET"]


@pytest.mark.parametrize("permissions", [[], [{"action": "USE", "constraint": []}]])
async def test_counteroffer_rejects_missing_terms_before_sending(permissions):
    fake = FakeConnector({})

    result = await call(fake, "send_counteroffer", negotiation_id=NEG_ID, permissions=permissions, connector="b")

    assert result.is_error
    assert fake.requests == []


async def test_counteroffer_puts_every_permission_into_the_one_offer():
    count_5 = {**COUNT_3, "constraint": [{**COUNT_3["constraint"][0], "rightOperand": "5"}]}
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): one(record("provider", assignee="urn:stored")),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/offer"): one(MINE),
        }
    )

    await call(fake, "send_counteroffer", negotiation_id=NEG_ID, permissions=[COUNT_3, count_5], connector="b")

    assert countered(fake, "offer")["permission"] == [COUNT_3, count_5]


ALREADY = FIXTURES["error_400_already"]["body"]  # live: "... FINALIZED state can not transition to FINALIZED"
# Live, connector A terminating twice: the peer's DSP error is relayed, the real reason only in data.reason.
TERMINATED_TWICE = {
    "success": False,
    "message": "consumer did not process Terminate message correct",
    "data": {
        "@type": "ContractNegotiationErrorMessage",
        "code": "Bad Request",
        "reason": ["State transition aborted, TERMINATED state can not transition to TERMINATED"],
    },
}


def states(*records):
    """A GET route answering with each record in turn (before the transition, then the re-read)."""
    answers = iter(records)
    return lambda _: httpx2.Response(200, json={"success": True, "data": next(answers)})


def at(state, role="provider"):
    return {**MINE, "state": state, "role": role}


@pytest.mark.parametrize(
    ("action", "role", "error_body", "state"),
    [
        ("finalize", "provider", ALREADY, "FINALIZED"),
        ("terminate", "consumer", TERMINATED_TWICE, "TERMINATED"),
    ],
)
async def test_repeated_transition_is_already_done(action, role, error_body, state):
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): states(at(state, role), at(state, role)),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/{action}"): lambda _: httpx2.Response(400, json=error_body),
        }
    )

    result = await call(fake, f"{action}_negotiation", negotiation_id=NEG_ID, connector="a")

    assert not result.is_error, result.content
    assert result.structured_content == {"already_done": True, "state": state, "negotiation": at(state, role)}


@pytest.mark.parametrize(
    ("action", "role", "before", "after"),
    [("verify", "consumer", "AGREED", "VERIFIED"), ("finalize", "provider", "VERIFIED", "FINALIZED")],
)
async def test_verify_rereads_the_negotiation(action, role, before, after):
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): states(at(before, role), at(after, role)),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/{action}"): "envelope_no_data",
        }
    )

    result = await call(fake, f"{action}_negotiation", negotiation_id=NEG_ID, connector="a")

    assert result.structured_content == {"already_done": False, "state": after, "negotiation": at(after, role)}
    assert [r.method for r in fake.requests] == ["GET", "PUT", "GET"]


async def test_agree_returns_the_record_it_gets_back_without_rereading():
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): one(at("REQUESTED")),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/agree"): one(at("AGREED")),
        }
    )

    result = await call(fake, "agree_negotiation", negotiation_id=NEG_ID, connector="a")

    assert result.structured_content == {"already_done": False, "state": "AGREED", "negotiation": at("AGREED")}
    assert [r.method for r in fake.requests] == ["GET", "PUT"]


@pytest.mark.parametrize(
    ("action", "side", "wrong"),
    [
        ("accept", "consumer", "provider"),
        ("agree", "provider", "consumer"),
        ("verify", "consumer", "provider"),
        ("finalize", "provider", "consumer"),
    ],
)
async def test_transition_on_the_wrong_side_is_refused_locally(action, side, wrong):
    fake = FakeConnector({("GET", f"/api/v1/negotiations/{NEG_ID}"): one(at("REQUESTED", wrong))})

    result = await call(fake, f"{action}_negotiation", negotiation_id=NEG_ID, connector="a")

    assert result.is_error
    assert f"{action} is a {side} action, but {NEG_ID} is the {wrong} record on connector 'a'" in result.content[0].text
    assert f"'a': call it on the {side}'s connector with find_counterpart_negotiation's @id" in result.content[0].text
    assert [r.method for r in fake.requests] == ["GET"]


@pytest.mark.parametrize("role", ["consumer", "provider"])
async def test_either_side_can_terminate(role):
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): one(at("REQUESTED", role)),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/terminate"): one(at("TERMINATED", role)),
        }
    )

    result = await call(fake, "terminate_negotiation", negotiation_id=NEG_ID, connector="a")

    assert result.structured_content["state"] == "TERMINATED"


async def test_an_illegal_transition_stays_an_error():
    illegal = {
        "success": False,
        "message": "State transition aborted, FINALIZED state can not transition to TERMINATED",
    }
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/negotiations/{NEG_ID}"): one(at("FINALIZED")),
            ("PUT", f"/api/v1/negotiations/{NEG_ID}/terminate"): lambda _: httpx2.Response(400, json=illegal),
        }
    )

    result = await call(fake, "terminate_negotiation", negotiation_id=NEG_ID, connector="a")

    assert result.is_error
    assert "FINALIZED state can not transition to TERMINATED" in result.content[0].text


async def test_transitions_describe_the_state_machine_and_have_an_output_schema():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}

    for action in ("accept", "agree", "verify", "finalize", "terminate"):
        tool = tools[f"{action}_negotiation"]
        assert "OFFERED -> REQUESTED | ACCEPTED | TERMINATED" in tool.description, action
        assert "already_done" in tool.description, action
        assert set(tool.output_schema["properties"]) == {"already_done", "state", "negotiation"}, action
