import contextlib
import logging
from collections.abc import AsyncGenerator
from typing import Any

import httpx2
from mcp.server.mcpserver import MCPServer
from mcp.types import Tool as MCPTool

from tc_mcp import prompts, resources
from tc_mcp.config import Settings
from tc_mcp.registry import Registry
from tc_mcp.tools import catalog, datasets, negotiation, ops, services, transfer, wait

# Sent once per session, instead of in every negotiation and transfer tool's description.
INSTRUCTIONS = f"""Negotiations and transfers move through these state machines.

{negotiation.STATE_MACHINE}

{transfer.STATE_MACHINE}"""


def _untitled(schema: Any) -> Any:
    """Drop pydantic's generated titles ("Dataset Id" for dataset_id): they repeat the name and cost tokens on every
    request. Property names are kept, so an argument called `title` survives."""
    if isinstance(schema, list):
        return [_untitled(item) for item in schema]
    if not isinstance(schema, dict):
        return schema
    out = {}
    for key, value in schema.items():
        if key == "properties":
            out[key] = {name: _untitled(sub) for name, sub in value.items()}
        elif key != "title":
            out[key] = _untitled(value)
    return out


class _Server(MCPServer):
    async def list_tools(self) -> list[MCPTool]:
        tools = await super().list_tools()
        for tool in tools:
            tool.input_schema = _untitled(tool.input_schema)
        return tools


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

    mcp = _Server("trueconnector", instructions=INSTRUCTIONS, lifespan=lifespan)
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
