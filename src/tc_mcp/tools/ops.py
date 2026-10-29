from typing import Any

from mcp.server.mcpserver import Context, MCPServer

from tc_mcp.tools import READ_ONLY, registry


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=READ_ONLY)
    async def get_dspace_version(ctx: Context, connector: str | None = None) -> Any:
        """Dataspace Protocol versions the connector supports (public endpoint, no login)."""
        return await registry(ctx).get(connector).request("GET", "/.well-known/dspace-version", auth=False)
