import contextlib
import json
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx2
import mcp
import pytest
from helpers import Call

from tc_mcp.client import TCClient
from tc_mcp.config import Settings
from tc_mcp.server import build_server


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


SUPER_ADMIN = {"email": "superadmin@mail.com", "password": "password"}  # the local stack's public demo account
AUTOMATION = ("automaticNegotiation", "automaticTransfer")


@pytest.fixture
def manual_negotiation() -> Iterator[None]:
    """Both tenants with automatic negotiation off; the previous flags are restored afterwards."""
    with automation_off("automaticNegotiation"):
        yield


@pytest.fixture
def manual_transfer() -> Iterator[None]:
    """Both tenants with automatic transfer off (negotiation stays as it is); restored afterwards."""
    with automation_off("automaticTransfer"):
        yield


@contextlib.contextmanager
def automation_off(flag: str) -> Iterator[None]:
    """Tenant flags need a SUPER_ADMIN, which the MCP server never has, so this calls the API directly.
    The PUT resets any flag it is not given, so all are always sent."""
    tenants = []
    for connector in stack_settings().connectors:
        http = httpx2.Client(base_url=connector.base_url, timeout=30)
        token = http.post("/api/v1/auth/login", json=SUPER_ADMIN).raise_for_status().json()["access_token"]
        http.headers["Authorization"] = f"Bearer {token}"
        path = f"/api/v1/tenants/{urlparse(connector.protocol_url).path.strip('/')}"
        flags = {k: http.get(path).raise_for_status().json()["data"][k] for k in AUTOMATION}
        tenants.append((http, path, flags))
    try:
        for http, path, flags in tenants:
            http.put(path, json=flags | {flag: False}).raise_for_status()
        yield
    finally:
        for http, path, flags in tenants:
            http.put(path, json=flags).raise_for_status()
            http.close()


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
