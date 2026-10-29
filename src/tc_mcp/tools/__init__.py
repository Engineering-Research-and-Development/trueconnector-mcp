from typing import cast

from mcp.server.mcpserver import Context

from tc_mcp.client import Registry


def registry(ctx: Context) -> Registry:
    """The connector registry the server's lifespan created."""
    return cast(Registry, ctx.request_context.lifespan_context)
