import contextlib
from collections.abc import AsyncIterator

import httpx2
from mcp.server.mcpserver import MCPServer

from tc_mcp import prompts, resources
from tc_mcp.client import Registry
from tc_mcp.config import Settings
from tc_mcp.tools import catalog, datasets, negotiation, ops, services


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
    catalog.register(mcp)
    services.register(mcp)
    datasets.register(mcp)
    negotiation.register(mcp)
    resources.register(mcp, settings)
    prompts.register(mcp)
    return mcp
