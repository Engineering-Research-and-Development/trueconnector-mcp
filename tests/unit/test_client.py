import httpx2
import pytest
from conftest import FIXTURES, FakeConnector, make_client
from mcp.server.mcpserver.exceptions import ToolError

from tc_mcp.client import AlreadyInState, Rejected, TCClient, _error, _unwrap
from tc_mcp.config import Connector

pytestmark = pytest.mark.anyio


async def test_unwraps_envelope_data():
    client = make_client(FakeConnector({("GET", "/api/v1/catalogs"): "envelope_data"}))

    catalog = await client.request("GET", "/api/v1/catalogs")

    assert catalog == FIXTURES["envelope_data"]["body"]["data"]


async def test_unwraps_paged_list_without_hateoas_links():
    client = make_client(FakeConnector({("GET", "/api/v1/negotiations"): "paged"}))

    result = await client.request("GET", "/api/v1/negotiations")

    recorded = FIXTURES["paged"]["body"]["response"]["data"]
    assert "links" in recorded["content"][0]
    assert set(result) == {"items", "page"}
    assert result["page"] == recorded["page"]
    assert len(result["items"]) == len(recorded["content"]) > 0
    assert all("links" not in item for item in result["items"])
    assert result["items"][0]["@id"] == recorded["content"][0]["@id"]


async def test_envelope_without_data_returns_message():
    client = make_client(FakeConnector({("DELETE", "/api/v1/dataservices/x"): "envelope_no_data"}))

    assert await client.request("DELETE", "/api/v1/dataservices/x") == {"message": "Data service deleted successfully"}


def test_empty_data_is_data_not_a_message():
    # No live connector had an empty list to record, so take the real envelope and empty its data.
    assert _unwrap({**FIXTURES["envelope_data"]["body"], "data": []}) == []


@pytest.mark.parametrize("name", ["login", "dspace_version", "view"])
async def test_bare_bodies_pass_through(name):
    client = make_client(FakeConnector({("GET", "/x"): name}))

    assert await client.request("GET", "/x") == FIXTURES[name]["body"]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("error_400", "rejected: Contract negotiation with id does-not-exist not found"),
        ("error_401", "authentication failed"),
        ("error_401_refresh_reuse", "authentication failed"),
        ("error_403", "forbidden"),
        ("error_500", "internal error (Cannot invoke"),
    ],
)
async def test_error_shapes_map_to_tool_errors(name, expected):
    client = make_client(FakeConnector({("GET", "/x"): name}))

    with pytest.raises(ToolError) as err:
        await client.request("GET", "/x")

    assert str(err.value).startswith("a GET /x: " if name != "error_400" else "a GET /x rejected: ")
    assert expected in str(err.value)
    assert isinstance(err.value, Rejected) == (name == "error_400")
    assert not isinstance(err.value, AlreadyInState)


def test_unexpected_status_is_reported_with_its_code():
    assert "unexpected HTTP 404" in str(_error(404, {"message": "nope"}, "a GET /x"))


async def test_same_state_transition_is_already_in_state():
    client = make_client(FakeConnector({("PUT", "/x"): "error_400_already"}))

    with pytest.raises(AlreadyInState) as err:
        await client.request("PUT", "/x")

    assert err.value.state == "FINALIZED"
    assert err.value.reason == FIXTURES["error_400_already"]["body"]["message"]


def test_illegal_transition_is_a_plain_rejection():
    message = "State transition aborted, FINALIZED state can not transition to ACCEPTED"  # live, connector A

    err = _error(400, {"success": False, "message": message}, "a PUT /x")

    assert isinstance(err, Rejected)
    assert not isinstance(err, AlreadyInState)


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (httpx2.ConnectError("refused"), r"^b POST /x: connector unreachable at http://tc-b"),
        (httpx2.ReadTimeout("slow"), r"^b POST /x: timed out; it may still have been applied"),
    ],
)
async def test_unreachable_connector(exc, expected):
    def down(request):
        raise exc

    client = TCClient(Connector(name="b", base_url="http://tc-b"), timeout_s=5, transport=httpx2.MockTransport(down))

    with pytest.raises(ToolError, match=expected):
        await client.request("POST", "/x")
