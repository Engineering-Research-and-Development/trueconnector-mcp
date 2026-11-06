import json
from collections.abc import AsyncIterator
from typing import Any

import httpx2
import mcp
import pytest
from helpers import Call

# The stack check and the tenant automation switches of the integration tests.
from integration.conftest import manual_negotiation, manual_transfer, require_stack  # noqa: F401

SERVER = "http://localhost:8000/mcp"  # the Dockerized server (compose.yaml), not an in-process one


@pytest.fixture(scope="session", autouse=True)
def require_server() -> None:
    try:
        httpx2.get(SERVER, timeout=3)
    except httpx2.HTTPError:
        pytest.skip(f"the Dockerized MCP server is down at {SERVER}: docker compose up -d --build")


@pytest.fixture
async def http_client() -> AsyncIterator[mcp.Client]:
    async with mcp.Client(SERVER) as client:
        yield client


@pytest.fixture
def call(http_client: mcp.Client) -> Call:
    async def call(tool: str, **arguments: Any) -> Any:
        result = await http_client.call_tool(tool, arguments)
        assert not result.is_error, result.content[0].text
        return json.loads(result.content[0].text)

    return call
