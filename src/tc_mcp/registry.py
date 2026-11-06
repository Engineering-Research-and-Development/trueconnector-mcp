from typing import cast

import httpx2
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError

from tc_mcp.client import TCClient
from tc_mcp.config import Settings


class Registry:
    """One TCClient per configured connector; `connector` may be omitted only when there is exactly one."""

    def __init__(self, settings: Settings, transport: httpx2.AsyncBaseTransport | None = None):
        self._clients = {
            c.name: TCClient(c, settings.timeout_s, transport, max_concurrency=settings.max_concurrency)
            for c in settings.connectors
        }
        self.moves_started: set[tuple[str, str]] = set()
        # Downloads dataset sources for store="copy"; never sends connector credentials.
        self.http = httpx2.AsyncClient(timeout=settings.timeout_s, transport=transport, follow_redirects=True)
        self.max_copy_bytes = int(settings.max_copy_mb * 1_000_000)

    def get(self, name: str | None = None) -> TCClient:
        if name is None:
            if len(self._clients) > 1:  # a silent default could act on the wrong side of an exchange
                raise ToolError(f"Several connectors configured ({', '.join(self._clients)}): pass connector")
            return next(iter(self._clients.values()))
        if name not in self._clients:
            raise ToolError(f"Unknown connector '{name}'. Configured: {', '.join(self._clients)}")
        return self._clients[name]

    def forward_to(self, peer: str) -> str:
        """The Forward-To value for `peer`: a configured connector's protocol_url, or an http(s) URL as given."""
        if peer in self._clients:
            url = self._clients[peer].connector.protocol_url
            if url is None:
                raise ToolError(f"Connector '{peer}' has no protocol_url configured; pass its DSP protocol URL instead")
            return url
        if peer.startswith(("http://", "https://")):
            return peer
        raise ToolError(
            f"Unknown peer '{peer}': use a configured connector name ({', '.join(self._clients)}) "
            "or the peer's DSP protocol URL, e.g. http://connector-b:8080/connector-b"
        )

    async def aclose(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        await self.http.aclose()


def registry(ctx: Context) -> Registry:
    """The connector registry the server's lifespan created."""
    return cast(Registry, ctx.request_context.lifespan_context)
