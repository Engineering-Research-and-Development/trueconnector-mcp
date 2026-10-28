import httpx2
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from tc_mcp.client import Rejected

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_live_login(client):
    catalog = await client.request("GET", "/api/v1/catalogs")

    assert "participantId" in catalog  # the envelope was unwrapped
    assert client._refresh is not None


async def test_forced_refresh_rotates_the_refresh_token(client):
    await client.request("GET", "/api/v1/catalogs")
    access, refresh = client._access, client._refresh
    client._expires_at = 0  # pretend the access token expired

    await client.request("GET", "/api/v1/catalogs")

    assert client._access != access and client._refresh != refresh
    # The used refresh token is revoked; a login fallback would have left it valid.
    reuse = httpx2.post(f"{client.connector.base_url}/api/v1/auth/refresh", json={"refresh_token": refresh})
    assert reuse.status_code == 401


async def test_not_found_is_rejected_with_the_connector_message(client):
    with pytest.raises(Rejected, match="not found") as e:
        await client.request("GET", "/api/v1/negotiations/does-not-exist")

    assert e.value.reason == "Contract negotiation with id does-not-exist not found"


async def test_admin_forbidden_route_maps_to_403_message(client):
    with pytest.raises(ToolError, match=r"^a GET /api/v1/properties/: forbidden"):
        await client.request("GET", "/api/v1/properties/")


async def test_dspace_version_tool_on_both_connectors(call):
    for connector in ("a", "b"):
        versions = await call("get_dspace_version", connector=connector)
        assert versions["protocolVersions"][0]["version"] == "2025-1"
