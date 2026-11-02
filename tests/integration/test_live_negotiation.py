import pytest

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_list_filters_and_find_counterpart(call):
    finalized = await call("list_negotiations", state="FINALIZED", role="consumer", size=5, connector="a")
    assert finalized["items"], "the stack has finalized negotiations from earlier runs"
    assert {(n["state"], n["role"]) for n in finalized["items"]} == {("FINALIZED", "consumer")}
    assert finalized["page"]["size"] == 5

    mine = finalized["items"][0]
    theirs = await call("find_counterpart_negotiation", id=mine["@id"], peer_connector="b", connector="a")
    assert (theirs["role"], theirs["consumerPid"], theirs["providerPid"]) == (
        "provider",
        mine["consumerPid"],
        mine["providerPid"],
    )
    assert theirs["@id"] != mine["@id"]  # each side keeps its own record

    back = await call("find_counterpart_negotiation", id=theirs["@id"], peer_connector="a", connector="b")
    assert back["@id"] == mine["@id"]

    by_pid = await call("list_negotiations", consumer_pid=mine["consumerPid"], connector="b")
    assert [n["@id"] for n in by_pid["items"]] == [theirs["@id"]]


async def test_validate_offer_and_enforce_agreement(call):
    catalog = await call("get_own_catalog", connector="b")
    dataset = catalog["dataset"][0]
    offer = dataset["hasPolicy"][0] | {"target": dataset["@id"], "assigner": catalog["participantId"]}

    assert await call("validate_offer", offer=offer, connector="b") == {"valid": True, "message": "Offer is valid"}
    wrong_target = offer | {"target": "urn:uuid:not-in-the-catalog"}
    assert await call("validate_offer", offer=wrong_target, connector="b") == {
        "valid": False,
        "message": "Offer not valid",
    }

    finalized = await call("list_negotiations", state="FINALIZED", role="consumer", size=1, connector="a")
    agreement_id = finalized["items"][0]["agreement"]["@id"]
    for side in ("a", "b"):  # both sides hold the agreement
        assert (await call("enforce_agreement", agreement_id=agreement_id, connector=side))["allowed"] is True


async def test_enforce_on_a_missing_agreement_is_an_error_not_a_denial(mcp_client):
    result = await mcp_client.call_tool("enforce_agreement", {"agreement_id": "urn:uuid:nope", "connector": "a"})

    assert result.is_error
    assert "not found (Agreement with Id urn:uuid:nope not found.)" in result.content[0].text
