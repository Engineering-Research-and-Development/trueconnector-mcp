from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ToolError

from tc_mcp.config import Settings
from tc_mcp.tools import registry
from tc_mcp.tools.catalog import seg


async def _get(ctx: Context, connector: str, path: str) -> Any:
    try:
        return await registry(ctx).get(connector).request("GET", path)
    except ToolError as e:  # a resource read reports a ResourceError; anything else reaches the client as a crash
        raise ResourceError(str(e)) from e


def register(mcp: MCPServer, settings: Settings) -> None:
    @mcp.resource("trueconnector://connectors", mime_type="application/json")
    def connectors() -> list[dict[str, Any]]:
        """The configured connectors (names to pass as `connector` or `peer`). No credentials."""
        return [
            {
                "name": c.name,
                "base_url": c.base_url,
                "protocol_url": c.protocol_url,
                "auth": "password" if c.password else "token" if c.token else "none",
            }
            for c in settings.connectors
        ]

    @mcp.resource("trueconnector://{connector}/catalog", mime_type="application/json")
    async def catalog(connector: str, ctx: Context) -> Any:
        """A connector's own catalog."""
        return await _get(ctx, connector, "/api/v1/catalogs")

    @mcp.resource("trueconnector://{connector}/datasets/{id}", mime_type="application/json")
    async def dataset(connector: str, id: str, ctx: Context) -> Any:
        """One of a connector's datasets."""
        return await _get(ctx, connector, f"/api/v1/datasets/{seg(id)}")
