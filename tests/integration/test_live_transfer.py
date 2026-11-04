import contextlib
import json

import httpx2
import pytest
from helpers import SOURCE_DATA

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


async def downloaded(call, tid, connector):
    waited = await call("wait_for_state", kind="transfer", id=tid, states=["DOWNLOADED"], connector=connector)
    assert waited["outcome"] == "reached", waited
    return waited["object"]


async def fetched(call, tid):
    """The consumer's copy of the data, fetched from the host through the presigned link."""
    url = (await call("get_transfer_data_url", transfer_process_id=tid, connector="a"))["url"]
    async with httpx2.AsyncClient() as http:  # the link names host.docker.internal, which Docker Desktop resolves
        response = await http.get(url)
    assert response.status_code == 200, response.text
    return response.json()


SEEDED_DATA = [{"employee": {"name": "John Doe", "age": 30, "city": "New York"}}]  # the stack's seeded dataset


@pytest.mark.parametrize(
    ("fmt", "mover", "other"), [("HttpData-PULL", "a", "b"), ("HttpData-PUSH", "b", "a")], ids=["pull", "push"]
)
async def test_move_data_on_the_right_side_and_get_its_url(call, mcp_client, manual_transfer, fmt, mover, other):
    mine, theirs = await requested_transfer(call, fmt)
    ids = {"a": mine, "b": theirs}
    early = await mcp_client.call_tool("move_transfer_data", {"transfer_process_id": ids[mover], "connector": mover})
    assert early.is_error
    assert "is REQUESTED: data moves only while STARTED" in early.content[0].text
    await call("start_transfer", transfer_process_id=theirs, connector="b")

    wrong = await mcp_client.call_tool("move_transfer_data", {"transfer_process_id": ids[other], "connector": other})
    assert wrong.is_error
    assert f"{fmt} data is moved by the" in wrong.content[0].text

    moved = await call("move_transfer_data", transfer_process_id=ids[mover], connector=mover)
    assert moved["accepted"] is True, moved

    transfer = await downloaded(call, mine, "a")
    assert transfer["state"] == "COMPLETED"  # a finished move completes the transfer, even in manual mode
    assert await fetched(call, mine) == SEEDED_DATA
    again = await call("move_transfer_data", transfer_process_id=ids[mover], connector=mover)
    assert again == {
        "accepted": False,
        "message": "the data is already downloaded: get_transfer_data_url gives its link",
    }


async def test_automatic_pull_is_waited_until_the_data_has_arrived(call):
    mine, _ = await requested_transfer(call, "HttpData-PULL")

    transfer = await downloaded(call, mine, "a")

    assert (transfer["state"], transfer["downloaded"]) == ("COMPLETED", True)
    assert await fetched(call, mine) == SEEDED_DATA


async def test_transfer_resource_reads_the_record(call, mcp_client):
    mine = (await call("list_transfers", state="COMPLETED", role="consumer", size=1, connector="a"))["items"][0]

    read = await mcp_client.read_resource(f"trueconnector://a/transfers/{mine['@id']}")

    assert json.loads(read.contents[0].text) == await call("get_transfer", id=mine["@id"], connector="a")


@contextlib.asynccontextmanager
async def published(call, **args):
    """A dataset published on b for one test, deleted afterwards with its distribution and data service."""
    offer = {"action": "USE", "constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": "5"}]}
    dataset = await call(
        "publish_dataset", title="mcp transfer", description="test", offers=[offer], connector="b", **args
    )
    [distribution] = dataset["distribution"]
    try:
        yield dataset
    finally:
        await call("delete_dataset", id=dataset["@id"], connector="b")
        await call("delete_distribution", id=distribution["@id"], connector="b")
        await call("delete_data_service", id=distribution["accessService"]["@id"], connector="b")


async def transfer_of(call, dataset_id, fmt):
    """Negotiates `dataset_id` from a and requests its transfer: (consumer @id, provider @id)."""
    negotiation = await call("request_contract", peer="b", dataset_id=dataset_id, connector="a")
    agreed = await call(
        "wait_for_state", kind="negotiation", id=negotiation["@id"], states=["FINALIZED"], connector="a"
    )
    agreement_id = agreed["object"]["agreement"]["@id"]
    mine = (await call("find_transfer_for_agreement", agreement_id=agreement_id, connector="a"))["@id"]
    theirs = (await call("find_transfer_for_agreement", agreement_id=agreement_id, connector="b"))["@id"]
    await call("request_transfer", transfer_process_id=mine, format=fmt, connector="a")
    return mine, theirs


@pytest.mark.parametrize("fmt", ["HttpData-PULL", "HttpData-PUSH"])
async def test_a_published_dataset_is_transferred_with_its_data(call, source_url, fmt):
    async with published(call, external_url=source_url, format=fmt) as dataset:
        mine, _ = await transfer_of(call, dataset["@id"], fmt)

        await downloaded(call, mine, "a")

        assert await fetched(call, mine) == json.loads(SOURCE_DATA)


async def test_a_failed_data_move_is_reported(call, manual_transfer):
    # A linked (EXTERNAL) source is the one way to make a move fail: the connector fetches it only during the move.
    async with published(call, external_url="http://unreachable.invalid/data.json", store="link") as dataset:
        mine, theirs = await transfer_of(call, dataset["@id"], "HttpData-PULL")
        await call("start_transfer", transfer_process_id=theirs, connector="b")
        assert (await call("move_transfer_data", transfer_process_id=mine, connector="a"))["accepted"] is True

        waited = await call("wait_for_state", kind="transfer", id=mine, states=["DOWNLOADED"], connector="a")

        # TrueConnector itself reports nothing: the transfer just stays STARTED without data.
        assert (waited["outcome"], waited["state"], waited["object"]["downloaded"]) == ("failed", "STARTED", False)
        assert "connector logs" in waited["hint"]
