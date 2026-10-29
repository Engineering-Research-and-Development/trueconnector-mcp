import json
import sys
from pathlib import Path

import httpx2
import mcp
import pytest
from dotenv import dotenv_values

pytestmark = [pytest.mark.integration, pytest.mark.anyio]

DOCKER_URL = "http://localhost:8000/mcp"


async def assert_serves_both_connectors(client: mcp.Client) -> None:
    for connector in ("a", "b"):
        result = await client.call_tool("get_dspace_version", {"connector": connector})
        assert not result.is_error, result.content[0].text
        assert json.loads(result.content[0].text)["protocolVersions"][0]["version"] == "2025-1"


async def test_stdio_subprocess():
    # The real entry point as a child process; the stack settings go in as env vars, which override any .env.
    env = dotenv_values(Path(__file__).parents[2] / ".env.example")
    params = mcp.StdioServerParameters(command=sys.executable, args=["-m", "tc_mcp"], env=env)
    async with mcp.Client(params) as client:
        await assert_serves_both_connectors(client)


async def test_streamable_http_in_docker():
    try:
        httpx2.get(DOCKER_URL, timeout=3)
    except httpx2.TransportError:
        pytest.skip(f"tc-mcp container not reachable at {DOCKER_URL} (docker compose up -d --build)")
    async with mcp.Client(DOCKER_URL) as client:
        await assert_serves_both_connectors(client)
