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


async def test_request_contract_starts_a_negotiation_with_the_provider(call):
    dataset = (await call("browse_remote_catalog", peer="b", connector="a"))["datasets"][0]

    mine = await call("request_contract", peer="b", dataset_id=dataset["id"], connector="a")

    assert (mine["role"], mine["state"], mine["offer"]["target"]) == ("consumer", "REQUESTED", dataset["id"])
    theirs = await call("find_counterpart_negotiation", id=mine["@id"], peer_connector="b", connector="a")
    assert theirs["role"] == "provider"


async def test_offer_contract_stores_the_assignee_on_both_sides(call):
    dataset_id = (await call("get_own_catalog", connector="b"))["dataset"][0]["@id"]

    mine = await call("offer_contract", peer="a", dataset_id=dataset_id, connector="b")

    assignee = "http://connector-a:8080/connector-a/consumer"
    assert (mine["role"], mine["state"], mine["offer"]["assignee"]) == ("provider", "OFFERED", assignee)
    theirs = await call("find_counterpart_negotiation", id=mine["@id"], peer_connector="a", connector="b")
    assert (theirs["role"], theirs["offer"]["assignee"]) == ("consumer", assignee)

    # Only the consumer can end it: TrueConnector stores the provider's record without a callbackAddress.
    ended = await call("terminate_negotiation", negotiation_id=theirs["@id"], connector="a")
    assert (ended["already_done"], ended["state"]) == (False, "TERMINATED")


def count(n):
    return {"action": "USE", "constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": str(n)}]}


async def test_counteroffers_both_ways_keep_the_assignee(call, manual_negotiation):
    dataset = (await call("browse_remote_catalog", peer="b", connector="a"))["datasets"][0]
    mine = await call("request_contract", peer="b", dataset_id=dataset["id"], connector="a")
    theirs = await call("find_counterpart_negotiation", id=mine["@id"], peer_connector="b", connector="a")
    assert theirs["state"] == "REQUESTED"  # manual mode: the provider has to act

    await call("send_counteroffer", negotiation_id=theirs["@id"], permissions=[count(3)], connector="b")

    mine = await call("get_negotiation", id=mine["@id"], connector="a")
    assert mine["state"] == "OFFERED"
    assert mine["offer"]["permission"][0]["constraint"][0]["rightOperand"] == "3"

    await call("send_counteroffer", negotiation_id=mine["@id"], permissions=[count(4)], connector="a")

    theirs = await call("get_negotiation", id=theirs["@id"], connector="b")
    assert theirs["state"] == "REQUESTED"
    assert theirs["offer"]["permission"][0]["constraint"][0]["rightOperand"] == "4"
    assert theirs["offer"]["assignee"] == "http://connector-a:8080/connector-a/consumer"  # agree will need it

    ended = await call("terminate_negotiation", negotiation_id=mine["@id"], connector="a")
    assert ended["state"] == "TERMINATED"
    assert (await call("get_negotiation", id=theirs["@id"], connector="b"))["state"] == "TERMINATED"
    again = await call("terminate_negotiation", negotiation_id=mine["@id"], connector="a")
    assert (again["already_done"], again["state"]) == (True, "TERMINATED")


async def test_manual_negotiation_from_counteroffer_to_a_usable_agreement(call, mcp_client, manual_negotiation):
    dataset = (await call("browse_remote_catalog", peer="b", connector="a"))["datasets"][0]
    mine = await call("request_contract", peer="b", dataset_id=dataset["id"], connector="a")
    theirs = await call("find_counterpart_negotiation", id=mine["@id"], peer_connector="b", connector="a")
    await call("send_counteroffer", negotiation_id=theirs["@id"], permissions=[count(3)], connector="b")

    wrong_side = await mcp_client.call_tool("agree_negotiation", {"negotiation_id": mine["@id"], "connector": "a"})
    assert wrong_side.is_error
    assert "agree is a provider action" in wrong_side.content[0].text

    steps = [
        ("accept", mine, "a", "ACCEPTED"),
        ("agree", theirs, "b", "AGREED"),
        ("verify", mine, "a", "VERIFIED"),
        ("finalize", theirs, "b", "FINALIZED"),
    ]
    for action, record, connector, state in steps:
        done = await call(f"{action}_negotiation", negotiation_id=record["@id"], connector=connector)
        assert (done["already_done"], done["state"]) == (False, state), action

    mine = await call("get_negotiation", id=mine["@id"], connector="a")
    assert mine["state"] == "FINALIZED"
    assert mine["agreement"]["permission"][0]["constraint"][0]["rightOperand"] == "3"
    enforced = await call("enforce_agreement", agreement_id=mine["agreement"]["@id"], connector="a")
    assert enforced["allowed"] is True
    repeat = await call("finalize_negotiation", negotiation_id=theirs["@id"], connector="b")
    assert (repeat["already_done"], repeat["state"]) == (True, "FINALIZED")
