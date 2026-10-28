import contextlib
from collections.abc import AsyncIterator

import httpx2
from mcp.server.mcpserver import MCPServer

from tc_mcp.client import Registry
from tc_mcp.config import Settings
from tc_mcp.tools import ops


def build_server(settings: Settings, transport: httpx2.AsyncBaseTransport | None = None) -> MCPServer:
    @contextlib.asynccontextmanager
    async def lifespan(_: MCPServer) -> AsyncIterator[Registry]:
        registry = Registry(settings, transport)
        try:
            yield registry
        finally:
            await registry.aclose()

    mcp = MCPServer("trueconnector", lifespan=lifespan)
    ops.register(mcp)
    return mcp
