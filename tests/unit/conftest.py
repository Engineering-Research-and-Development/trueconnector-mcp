import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anyio
import httpx2

from tc_mcp.client import TCClient
from tc_mcp.config import Connector

FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "live_responses.json").read_text("utf-8"))


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
