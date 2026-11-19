import contextlib
import logging
from collections.abc import AsyncGenerator

import httpx2
from mcp.server.mcpserver import MCPServer

from tc_mcp import prompts, resources
from tc_mcp.config import Settings
from tc_mcp.registry import Registry
from tc_mcp.tools import catalog, datasets, negotiation, ops, services, transfer, wait

# Sent once per session, instead of in every negotiation and transfer tool's description.
INSTRUCTIONS = f"""Negotiations and transfers move through these state machines.

{negotiation.STATE_MACHINE}

{transfer.STATE_MACHINE}"""


def build_server(settings: Settings, transport: httpx2.AsyncBaseTransport | None = None) -> MCPServer:
    @contextlib.asynccontextmanager
    async def lifespan(server: MCPServer) -> AsyncGenerator[Registry]:
        if settings.read_only:
            for tool in await server.list_tools():
                if not (tool.annotations and tool.annotations.read_only_hint):
                    server.remove_tool(tool.name)
        registry = Registry(settings, transport)
        try:
            yield registry
        finally:
            await registry.aclose()

    mcp = MCPServer("trueconnector", instructions=INSTRUCTIONS, lifespan=lifespan)
    logging.getLogger("httpx2").setLevel(logging.WARNING)
    ops.register(mcp)
    catalog.register(mcp)
    services.register(mcp)
    datasets.register(mcp)
    negotiation.register(mcp)
    transfer.register(mcp)
    wait.register(mcp)
    resources.register(mcp, settings)
    prompts.register(mcp, settings)
    return mcp
