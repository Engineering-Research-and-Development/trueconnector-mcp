import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx2
import mcp
import pytest

from tc_mcp.client import TCClient
from tc_mcp.config import Settings
from tc_mcp.server import build_server

Call = Callable[..., Awaitable[Any]]


def stack_settings() -> Settings:
    """The local docker stack, as described by the committed .env.example (public demo credentials)."""
    return Settings(_env_file=Path(__file__).parents[2] / ".env.example")


@pytest.fixture(scope="session", autouse=True)
def require_stack() -> None:
    for connector in stack_settings().connectors:
        try:
            httpx2.get(f"{connector.base_url}/.well-known/dspace-version", timeout=3).raise_for_status()
        except httpx2.HTTPError:
            pytest.skip(f"TrueConnector stack is down ({connector.name} at {connector.base_url})")


@pytest.fixture
async def client() -> AsyncIterator[TCClient]:
    """A TCClient for connector a, logged out and closed after the test."""
    settings = stack_settings()
    client = TCClient(settings.connectors[0], settings.timeout_s)
    yield client
    await client.aclose()


@pytest.fixture
async def mcp_client() -> AsyncIterator[mcp.Client]:
    async with mcp.Client(build_server(stack_settings())) as client:
        yield client


@pytest.fixture
def call(mcp_client: mcp.Client) -> Call:
    """Calls a tool and returns its JSON result; a tool error fails the test with the model-facing message."""

    async def call(tool: str, **arguments: Any) -> Any:
        result = await mcp_client.call_tool(tool, arguments)
        assert not result.is_error, result.content[0].text
        return json.loads(result.content[0].text)

    return call
