"""Shared test helpers (importable from any test: `pythonpath = ["tests"]` in pyproject)."""

import json
from collections.abc import Awaitable, Callable
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any

import anyio
import httpx2
import mcp
from mcp.types import CallToolResult

from tc_mcp.client import TCClient
from tc_mcp.config import Connector, Settings
from tc_mcp.server import build_server

Call = Callable[..., Awaitable[Any]]

FIXTURES = json.loads((Path(__file__).parent / "unit" / "fixtures" / "live_responses.json").read_text("utf-8"))
SERVICE = FIXTURES["data_service"]["body"]["data"]
DISTRIBUTION = FIXTURES["distribution"]["body"]["data"]
DATASET = FIXTURES["dataset"]["body"]["data"]
COUNT_5 = {"action": "USE", "constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": "5"}]}

B_PROTOCOL = "http://connector-b:8080/connector-b"
SOURCE_DATA = b'{"source": "served by the test", "values": [1, 2, 3]}'

TWO_CONNECTORS = Settings(
    _env_file=None,
    connectors=[
        {"name": "a", "base_url": "http://tc-a"},
        {"name": "b", "base_url": "http://tc-b", "protocol_url": B_PROTOCOL},
    ],
)


def live(name: str) -> httpx2.Response:
    """A fresh copy of a recorded response, byte for byte as the connector sent it."""
    fx = FIXTURES[name]
    body = fx["body"] if isinstance(fx["body"], str) else json.dumps(fx["body"])
    return httpx2.Response(fx["status"], headers={"Content-Type": fx["content_type"]}, content=body.encode())


class FakeConnector:
    """Answers `(method, path)` with a recorded fixture and records every request it receives."""

    def __init__(self, routes: dict[tuple[str, str], str | Callable[[httpx2.Request], httpx2.Response]]):
        self.routes = routes
        self.requests: list[httpx2.Request] = []

    async def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        await anyio.sleep(0)
        route = self.routes[(request.method, request.url.path)]
        return live(route) if isinstance(route, str) else route(request)

    def calls(self, method: str, path: str) -> list[httpx2.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


def login_response(access: str, refresh: str, expires_in: int = 900) -> httpx2.Response:
    """The recorded login body with chosen tokens, so tests can tell token pairs apart."""
    body = {**FIXTURES["login"]["body"], "access_token": access, "refresh_token": refresh, "expires_in": expires_in}
    return httpx2.Response(200, json=body)


def make_client(fake: FakeConnector, **connector: Any) -> TCClient:
    conn = Connector(**{"name": "a", "base_url": "http://tc", **connector})
    return TCClient(conn, timeout_s=5, transport=httpx2.MockTransport(fake))


async def call(fake: FakeConnector, tool: str, **arguments: Any) -> CallToolResult:
    """Calls a tool on a server for connectors a and b whose HTTP traffic goes to `fake`."""
    async with mcp.Client(build_server(TWO_CONNECTORS, httpx2.MockTransport(fake))) as client:
        return await client.call_tool(tool, arguments)


def form_parts(request: httpx2.Request) -> dict[str, str]:
    """The multipart/form-data fields of a request, decoded with the stdlib email parser."""
    head = f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode()
    message = BytesParser(policy=default).parsebytes(head + request.content)
    return {
        p.get_param("name", header="content-disposition"): p.get_payload(decode=True).decode()
        for p in message.iter_parts()
    }
