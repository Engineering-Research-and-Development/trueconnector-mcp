import pytest
from conftest import FIXTURES, FakeConnector, make_client

from tc_mcp.client import _unwrap

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
