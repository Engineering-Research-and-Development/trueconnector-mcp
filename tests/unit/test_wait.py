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
    }
    assert len(fake.requests) == 3


async def test_stops_at_a_final_state_not_asked_for():
    fake = FakeConnector({("GET", PATH): states("REQUESTED", "TERMINATED")})

    result = await wait(fake, states=["FINALIZED"], interval_s=0.5)

    assert (result.structured_content["outcome"], result.structured_content["state"]) == ("final", "TERMINATED")
    assert len(fake.requests) == 2


async def test_times_out_with_the_last_state_seen():
    fake = FakeConnector({("GET", PATH): states("REQUESTED")})

    started = anyio.current_time()
    with anyio.fail_after(5):  # a wait that ignores its timeout must fail the test, not hang it
        result = await wait(fake, states=["FINALIZED"], timeout_s=0.6, interval_s=0.5)

    assert (result.structured_content["outcome"], result.structured_content["state"]) == ("timeout", "REQUESTED")
    assert len(fake.requests) == 3  # at 0 s, 0.5 s and the deadline
    assert anyio.current_time() - started < 0.9  # the last sleep is cut short at the deadline


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
