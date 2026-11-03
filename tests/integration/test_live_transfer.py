import pytest

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def finalized_agreement(call):
    dataset = (await call("browse_remote_catalog", peer="b", connector="a"))["datasets"][0]
    mine = await call("request_contract", peer="b", dataset_id=dataset["id"], connector="a")
    waited = await call("wait_for_state", kind="negotiation", id=mine["@id"], states=["FINALIZED"], connector="a")
    assert waited["outcome"] == "reached", waited
    return waited["object"]["agreement"]["@id"]


async def test_find_and_request_a_transfer(call):
    agreement_id = await finalized_agreement(call)

    mine = await call("find_transfer_for_agreement", agreement_id=agreement_id, connector="a")
    theirs = await call("find_transfer_for_agreement", agreement_id=agreement_id, connector="b")
    assert (mine["role"], mine["state"]) == ("consumer", "INITIALIZED")
    # Before the request the provider only holds a placeholder consumerPid: the agreement is the link.
    assert (theirs["role"], theirs["agreementId"], theirs["consumerPid"]) == (
        "provider",
        agreement_id,
        "temporary_consumer_pid",
    )

    listed = await call("list_transfers", agreement_id=agreement_id, role="provider", connector="b")
    assert [t["@id"] for t in listed["items"]] == [theirs["@id"]]

    requested = await call("request_transfer", transfer_process_id=mine["@id"], format="HttpData-PULL", connector="a")
    assert (requested["already_done"], requested["state"]) == (False, "REQUESTED")
    assert requested["transfer"]["format"] == "HttpData-PULL"
    assert (await call("get_transfer", id=mine["@id"], connector="a"))["format"] == "HttpData-PULL"
    assert (await call("get_transfer", id=theirs["@id"], connector="b"))["consumerPid"] == mine["consumerPid"]


async def test_request_transfer_from_the_provider_side_is_refused(call, mcp_client):
    agreement_id = await finalized_agreement(call)
    theirs = await call("find_transfer_for_agreement", agreement_id=agreement_id, connector="b")

    result = await mcp_client.call_tool(
        "request_transfer", {"transfer_process_id": theirs["@id"], "format": "HttpData-PULL", "connector": "b"}
    )

    assert result.is_error
    assert "request is a consumer action" in result.content[0].text


async def requested_transfer(call, format):
    agreement_id = await finalized_agreement(call)
    mine = await call("find_transfer_for_agreement", agreement_id=agreement_id, connector="a")
    theirs = await call("find_transfer_for_agreement", agreement_id=agreement_id, connector="b")
    await call("request_transfer", transfer_process_id=mine["@id"], format=format, connector="a")
    return mine["@id"], theirs["@id"]


async def test_manual_transfer_lifecycle(call, mcp_client, manual_transfer):
    mine, theirs = await requested_transfer(call, "HttpData-PULL")

    wrong_side = await mcp_client.call_tool("start_transfer", {"transfer_process_id": mine, "connector": "a"})
    assert wrong_side.is_error
    assert "consumer can not transit from REQUESTED to STARTED" in wrong_side.content[0].text

    steps = [
        ("start", theirs, "b", "STARTED"),
        ("suspend", mine, "a", "SUSPENDED"),
        ("start", mine, "a", "STARTED"),  # either side resumes
        ("complete", theirs, "b", "COMPLETED"),
    ]
    for action, tid, connector, state in steps:
        done = await call(f"{action}_transfer", transfer_process_id=tid, connector=connector)
        assert (done["already_done"], done["state"]) == (False, state), action
    assert (await call("get_transfer", id=mine, connector="a"))["state"] == "COMPLETED"

    again = await call("complete_transfer", transfer_process_id=mine, connector="a")
    assert (again["already_done"], again["state"]) == (True, "COMPLETED")


async def test_provider_terminates_a_requested_transfer(call, manual_transfer):
    mine, theirs = await requested_transfer(call, "HttpData-PUSH")

    ended = await call("terminate_transfer", transfer_process_id=theirs, connector="b")

    assert (ended["already_done"], ended["state"]) == (False, "TERMINATED")
    assert (await call("get_transfer", id=mine, connector="a"))["state"] == "TERMINATED"
    again = await call("terminate_transfer", transfer_process_id=theirs, connector="b")
    assert again["already_done"] is True
