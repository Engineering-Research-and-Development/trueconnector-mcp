from typing import Any

from mcp.server.mcpserver import Context, MCPServer

from tc_mcp.registry import registry
from tc_mcp.tools.common import REPEATABLE, WRITE, Format, fields, register_readers, seg


async def create_data_service(
    ctx: Context,
    title: str,
    description: str | None = None,
    keywords: list[str] | None = None,
    connector: str | None = None,
) -> Any:
    """Create a data service, the access point a distribution points to.

    The connector always sets its endpointURL to its own protocol URL.
    """
    body = fields(title, description, keywords)
    return await registry(ctx).get(connector).request("POST", "/api/v1/dataservices", json=body)


async def update_data_service(
    ctx: Context,
    id: str,
    title: str | None = None,
    description: str | None = None,
    keywords: list[str] | None = None,
    connector: str | None = None,
) -> Any:
    """Change the given fields of a data service; the others are kept."""
    # Data service PUT merges server-side, so only the given fields are sent.
    path = f"/api/v1/dataservices/{seg(id)}"
    return await registry(ctx).get(connector).request("PUT", path, json=fields(title, description, keywords))


async def create_distribution(
    ctx: Context,
    title: str,
    format: Format,
    access_service_id: str,
    description: str | None = None,
    connector: str | None = None,
) -> Any:
    """Create a distribution: how a dataset is delivered (format) and through which data service."""
    client = registry(ctx).get(connector)
    service = await client.request("GET", f"/api/v1/dataservices/{seg(access_service_id)}")
    body = {**fields(title, description), "format": format, "accessService": service}
    return await client.request("POST", "/api/v1/distributions", json=body)


async def update_distribution(
    ctx: Context,
    id: str,
    title: str | None = None,
    format: Format | None = None,
    access_service_id: str | None = None,
    description: str | None = None,
    connector: str | None = None,
) -> Any:
    """Change the given fields of a distribution; the others are kept."""
    # Distribution PUT replaces the whole object, so merge onto the current one.
    client = registry(ctx).get(connector)
    path = f"/api/v1/distributions/{seg(id)}"
    distribution = {**await client.request("GET", path), **fields(title, description)}
    if format:
        distribution["format"] = format
    if access_service_id:
        distribution["accessService"] = await client.request("GET", f"/api/v1/dataservices/{seg(access_service_id)}")
    return await client.request("PUT", path, json=distribution)


def register(mcp: MCPServer) -> None:
    for write in (create_data_service, create_distribution):
        mcp.add_tool(write, annotations=WRITE)
    for update in (update_data_service, update_distribution):
        mcp.add_tool(update, annotations=REPEATABLE)
    register_readers(mcp, "distribution", "distributions", "/api/v1/distributions")
    register_readers(mcp, "data_service", "data_services", "/api/v1/dataservices")
