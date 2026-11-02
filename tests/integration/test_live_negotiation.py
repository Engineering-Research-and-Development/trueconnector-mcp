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
