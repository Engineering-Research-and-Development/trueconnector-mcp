import json

import httpx2
import mcp
import pytest
from conftest import FIXTURES, FakeConnector

from tc_mcp.config import Settings
from tc_mcp.server import build_server

pytestmark = pytest.mark.anyio


class ClosableTransport(httpx2.MockTransport):
    closed = False

    async def aclose(self):
        self.closed = True


async def test_dspace_version_over_mcp():
    # Credentials are set but there is no login route: the tool must not authenticate.
    fake = FakeConnector({("GET", "/.well-known/dspace-version"): "dspace_version"})
    settings = Settings(
        _env_file=None,
        connectors=[{"name": "a", "base_url": "http://tc", "email": "admin@mail.com", "password": "pw"}],
    )

    transport = ClosableTransport(fake)

    async with mcp.Client(build_server(settings, transport)) as client:
        result = await client.call_tool("get_dspace_version", {})

    assert not result.is_error, result.content
    assert json.loads(result.content[0].text) == FIXTURES["dspace_version"]["body"]
    assert len(fake.requests) == 1
    assert transport.closed  # the lifespan closed the registry on shutdown
