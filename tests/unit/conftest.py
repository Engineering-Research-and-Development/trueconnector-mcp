import json
from pathlib import Path

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

    def __init__(self, routes: dict[tuple[str, str], str]):
        self.routes = routes
        self.requests: list[httpx2.Request] = []

    async def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        await anyio.sleep(0)
        return live(self.routes[(request.method, request.url.path)])


def make_client(fake: FakeConnector) -> TCClient:
    return TCClient(Connector(name="a", base_url="http://tc"), timeout_s=5, transport=httpx2.MockTransport(fake))
