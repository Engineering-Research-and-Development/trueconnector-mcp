import json

import anyio
import httpx2
import mcp
import pytest
from helpers import FIXTURES, TWO_CONNECTORS, FakeConnector, call

from tc_mcp.server import build_server

pytestmark = pytest.mark.anyio

RECORD = {k: v for k, v in FIXTURES["paged"]["body"]["response"]["data"]["content"][0].items() if k != "links"}
NEG_ID = RECORD["@id"]
PATH = f"/api/v1/negotiations/{NEG_ID}"


def states(*names):
    """Answers each poll with the next state; the last one repeats."""
    answers = list(names)

    def answer(_):
        state = answers.pop(0) if len(answers) > 1 else answers[0]
        return httpx2.Response(200, json={"success": True, "data": {**RECORD, "state": state}})

    return answer


async def wait(fake, **args):
    return await call(fake, "wait_for_state", kind="negotiation", id=NEG_ID, connector="a", **args)


async def test_waits_until_a_requested_state_is_reached():
    fake = FakeConnector({("GET", PATH): states("REQUESTED", "AGREED", "VERIFIED")})

    result = await wait(fake, states=["VERIFIED", "FINALIZED"], interval_s=0.5)

    assert not result.is_error, result.content
    assert result.structured_content == {
        "outcome": "reached",
        "state": "VERIFIED",
        "object": {**RECORD, "state": "VERIFIED"},
        "hint": None,
    }
    assert len(fake.requests) == 3


async def test_stops_at_a_final_state_not_asked_for():
    fake = FakeConnector({("GET", PATH): states("REQUESTED", "TERMINATED")})

    result = await wait(fake, states=["FINALIZED"], interval_s=0.5)

    assert (result.structured_content["outcome"], result.structured_content["state"]) == ("final", "TERMINATED")
    assert len(fake.requests) == 2


async def test_times_out_with_the_last_state_seen():
    answer, polled_at = states("REQUESTED"), []

    def timed(request):
        polled_at.append(anyio.current_time())
        return answer(request)

    fake = FakeConnector({("GET", PATH): timed})

    with anyio.fail_after(5):  # a wait that ignores its timeout must fail the test, not hang it
        result = await wait(fake, states=["FINALIZED"], timeout_s=0.6, interval_s=0.5)

    assert (result.structured_content["outcome"], result.structured_content["state"]) == ("timeout", "REQUESTED")
    assert len(polled_at) == 3  # at 0 s, 0.5 s and the deadline
    assert polled_at[-1] - polled_at[0] < 0.9  # the last sleep is cut short at the deadline (else 1.0 s)


async def test_reports_progress_while_waiting():
    fake = FakeConnector({("GET", PATH): states("REQUESTED", "AGREED", "FINALIZED")})
    progress = []

    async def on_progress(done, total, message):
        progress.append((total, message))

    async with mcp.Client(build_server(TWO_CONNECTORS, httpx2.MockTransport(fake))) as client:
        args = {"kind": "negotiation", "id": NEG_ID, "states": ["FINALIZED"], "interval_s": 0.5, "connector": "a"}
        result = await client.call_tool("wait_for_state", args, progress_callback=on_progress)

    assert result.structured_content["outcome"] == "reached"
    assert progress == [(30, f"negotiation {NEG_ID} is REQUESTED"), (30, f"negotiation {NEG_ID} is AGREED")]


@pytest.mark.parametrize(
    "bad",
    [
        {"states": ["finalized"]},
        {"states": []},
        {"kind": "transfer"},
        {"timeout_s": 121},
        {"timeout_s": 0},
        {"interval_s": 0.1},
    ],
)
async def test_rejects_unknown_states_and_bad_limits_before_polling(bad):
    fake = FakeConnector({})

    result = await call(
        fake, "wait_for_state", **({"kind": "negotiation", "id": NEG_ID, "states": ["FINALIZED"]} | bad), connector="a"
    )

    assert result.is_error
    assert fake.requests == []


TID = "urn:uuid:720f3f3a-d5de-43b0-989d-80d42fae8665"
TRANSFER_PATH = f"/api/v1/transfers/{TID}"


def transfer(state="STARTED", downloaded=False, running=False):
    return {"@id": TID, "role": "consumer", "format": "HttpData-PULL", "state": state} | {
        "downloaded": downloaded,
        "downloadInProgress": running,
    }


def polls(*records):
    """Answers each GET of the transfer with the next record; the last one repeats."""
    answers = list(records)

    def answer(_):
        record = answers.pop(0) if len(answers) > 1 else answers[0]
        return httpx2.Response(200, json={"success": True, "data": record})

    return answer


async def wait_transfer(fake, states, **args):
    return await call(
        fake, "wait_for_state", kind="transfer", id=TID, states=states, interval_s=0.5, connector="a", **args
    )


@pytest.mark.parametrize(
    "downloaded", [transfer(downloaded=True), transfer("COMPLETED", downloaded=True)], ids=["started", "completed"]
)
async def test_wait_downloaded_pseudo_state(downloaded):
    fake = FakeConnector({("GET", TRANSFER_PATH): polls(transfer(), transfer(running=True), downloaded)})

    result = await wait_transfer(fake, ["DOWNLOADED"])

    assert (result.structured_content["outcome"], result.structured_content["state"]) == (
        "reached",
        downloaded["state"],
    )
    assert len(fake.requests) == 3


async def test_wait_stops_at_a_completed_transfer_without_data():
    fake = FakeConnector({("GET", TRANSFER_PATH): polls(transfer("COMPLETED"))})

    result = await wait_transfer(fake, ["DOWNLOADED"])

    assert result.structured_content["outcome"] == "final"


async def test_wait_detects_failed_move_started_by_this_server():
    accepted = {"success": True, "message": "Request accepted", "data": f"Download started for transfer process {TID}"}
    fake = FakeConnector(
        {
            ("GET", TRANSFER_PATH): polls(transfer(), transfer()),  # move_transfer_data's guard read, then the wait
            ("GET", f"{TRANSFER_PATH}/download"): lambda _: httpx2.Response(202, json=accepted),
        }
    )

    async with mcp.Client(build_server(TWO_CONNECTORS, httpx2.MockTransport(fake))) as client:
        moved = await client.call_tool("move_transfer_data", {"transfer_process_id": TID, "connector": "a"})
        args = {"kind": "transfer", "id": TID, "states": ["DOWNLOADED"], "interval_s": 0.5, "connector": "a"}
        result = await client.call_tool("wait_for_state", args)

    assert moved.structured_content["accepted"] is True
    assert result.structured_content["outcome"] == "failed"
    assert "ended without data" in result.structured_content["hint"]
    assert len(fake.calls("GET", TRANSFER_PATH)) == 2  # failed on the first poll, no waiting


async def test_wait_detects_failure_after_seeing_the_download_run():
    fake = FakeConnector({("GET", TRANSFER_PATH): polls(transfer(), transfer(running=True), transfer())})

    result = await wait_transfer(fake, ["DOWNLOADED"])

    assert result.structured_content["outcome"] == "failed"
    assert len(fake.requests) == 3


async def test_wait_does_not_call_an_unstarted_move_failed():
    fake = FakeConnector({("GET", TRANSFER_PATH): polls(transfer())})

    with anyio.fail_after(5):
        result = await wait_transfer(fake, ["DOWNLOADED"], timeout_s=0.6)

    assert (result.structured_content["outcome"], result.structured_content["hint"]) == ("timeout", None)


@pytest.mark.parametrize(
    ("kind", "states"), [("transfer", ["FINALIZED"]), ("negotiation", ["DOWNLOADED"]), ("negotiation", ["STARTED"])]
)
async def test_wait_rejects_states_of_the_other_kind_before_polling(kind, states):
    fake = FakeConnector({})

    result = await call(fake, "wait_for_state", kind=kind, id=TID, states=states, connector="a")

    assert result.is_error
    assert f"are not {kind} states" in result.content[0].text
    assert fake.requests == []


async def test_wait_on_the_other_side_of_a_move_never_calls_it_failed():
    # PUSH: the provider (b) moves the data; the consumer's record (a) never shows the move running.
    accepted = {"success": True, "message": "Request accepted", "data": "Download started"}
    pushing = {**transfer(), "role": "provider", "format": "HttpData-PUSH"}
    fake = FakeConnector(
        {
            ("GET", TRANSFER_PATH): polls(pushing, transfer()),
            ("GET", f"{TRANSFER_PATH}/download"): lambda _: httpx2.Response(202, json=accepted),
        }
    )

    async with mcp.Client(build_server(TWO_CONNECTORS, httpx2.MockTransport(fake))) as client:
        await client.call_tool("move_transfer_data", {"transfer_process_id": TID, "connector": "b"})
        args = {"kind": "transfer", "id": TID, "states": ["DOWNLOADED"], "timeout_s": 0.6, "interval_s": 0.5}
        with anyio.fail_after(5):
            result = await client.call_tool("wait_for_state", args | {"connector": "a"})

    assert result.structured_content["outcome"] == "timeout"


async def test_the_input_schema_lists_every_state_a_client_may_wait_for():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        [tool] = [t for t in (await client.list_tools()).tools if t.name == "wait_for_state"]

    listed = json.dumps(tool.input_schema["properties"]["states"])
    for state in ("FINALIZED", "OFFERED", "STARTED", "SUSPENDED", "DOWNLOADED"):
        assert f'"{state}"' in listed, state
