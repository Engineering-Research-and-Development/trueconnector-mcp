from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.types import ToolAnnotations

from tc_mcp.tools import registry


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=ToolAnnotations(read_only_hint=True))
    async def get_dspace_version(ctx: Context, connector: str | None = None) -> Any:
        """Dataspace Protocol versions the connector supports (public endpoint, no login)."""
        return await registry(ctx).get(connector).request("GET", "/.well-known/dspace-version", auth=False)
