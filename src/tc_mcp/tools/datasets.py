import contextlib
import json
from typing import Annotated, Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from tc_mcp.client import TCClient
from tc_mcp.tools import READ_ONLY, WRITE, Permission, fields, register_readers, registry, seg
from tc_mcp.tools.services import Format

HttpUrl = Annotated[str, Field(pattern=r"^https?://\S+$")]
DistributionIds = Annotated[list[str], Field(min_length=1)]
Offers = Annotated[
    list[Permission], Field(min_length=1, description="Each permission becomes one offer a consumer can pick")
]


def _dataset_parts(
    dataset: dict[str, Any], external_url: str | None = None, external_authorization: str | None = None
) -> dict[str, Any]:
    """The multipart form for POST/PUT /api/v1/datasets (a JSON body gives 500)."""
    parts: dict[str, Any] = {"dataset": (None, json.dumps(dataset), "application/json")}
    if external_url:
        parts["url"] = (None, external_url)
    if external_authorization:
        parts["authorization"] = (None, external_authorization)
    return parts


async def _distributions(client: TCClient, ids: list[str]) -> list[Any]:
    """Full distribution objects: the connector rejects a dataset whose distributions are bare {"@id"} references."""
    return [await client.request("GET", f"/api/v1/distributions/{seg(i)}") for i in ids]


def _offers(permissions: list[Permission]) -> list[dict[str, Any]]:
    return [{"permission": [p.model_dump()]} for p in permissions]


async def create_dataset(
    ctx: Context,
    title: str,
    description: str,
    distribution_ids: DistributionIds,
    offers: Offers,
    external_url: HttpUrl,
    external_authorization: str | None = None,
    keywords: list[str] | None = None,
    connector: str | None = None,
) -> Any:
    """Publish a dataset in this connector's catalog (it is linked automatically).

    The data stays at `external_url`; the connector fetches it (sending `external_authorization` as the
    Authorization header, if given) when a consumer with an agreement downloads it.
    """
    client = registry(ctx).get(connector)
    dataset = {
        **fields(title, description, keywords),
        "distribution": await _distributions(client, distribution_ids),
        "hasPolicy": _offers(offers),
    }
    files = _dataset_parts(dataset, external_url, external_authorization)
    return await client.request("POST", "/api/v1/datasets", files=files)


async def update_dataset(
    ctx: Context,
    id: str,
    title: str | None = None,
    description: str | None = None,
    keywords: list[str] | None = None,
    distribution_ids: DistributionIds | None = None,
    offers: Offers | None = None,
    external_url: HttpUrl | None = None,
    external_authorization: str | None = None,
    connector: str | None = None,
) -> Any:
    """Change the given fields of a dataset; the others are kept.

    New `offers` replace all current offers and get new IDs. A new `external_url` replaces the artifact.
    """
    # Dataset PUT replaces the whole object, so merge onto the current one.
    client = registry(ctx).get(connector)
    path = f"/api/v1/datasets/{seg(id)}"
    dataset = {**await client.request("GET", path), **fields(title, description, keywords)}
    if distribution_ids:
        dataset["distribution"] = await _distributions(client, distribution_ids)
    if offers:
        dataset["hasPolicy"] = _offers(offers)
    return await client.request("PUT", path, files=_dataset_parts(dataset, external_url, external_authorization))


async def publish_dataset(
    ctx: Context,
    title: str,
    description: str,
    offers: Offers,
    external_url: HttpUrl,
    format: Format = "HttpData-PULL",
    external_authorization: str | None = None,
    keywords: list[str] | None = None,
    connector: str | None = None,
) -> Any:
    """Publish data that lives at `external_url` in one call: data service, distribution and dataset.

    Ask the user for the usage policy (`offers`) rather than inventing one. If any step fails, what this
    call created so far is deleted again, so nothing half-published is left behind. Returns the dataset.
    """
    client = registry(ctx).get(connector)
    created: list[str] = []
    try:
        service = await client.request("POST", "/api/v1/dataservices", json=fields(f"{title} service"))
        created.append(f"/api/v1/dataservices/{seg(service['@id'])}")
        body = {**fields(f"{title} ({format})"), "format": format, "accessService": service}
        distribution = await client.request("POST", "/api/v1/distributions", json=body)
        created.append(f"/api/v1/distributions/{seg(distribution['@id'])}")
        dataset = {
            **fields(title, description, keywords),
            "distribution": [distribution],
            "hasPolicy": _offers(offers),
        }
        files = _dataset_parts(dataset, external_url, external_authorization)
        return await client.request("POST", "/api/v1/datasets", files=files)
    except ToolError as e:
        for path in reversed(created):
            with contextlib.suppress(ToolError):
                await client.request("DELETE", path)
        raise ToolError(f"{e} (publish_dataset deleted the {len(created)} object(s) it had created)") from e


async def get_dataset_formats(ctx: Context, id: str, connector: str | None = None) -> Any:
    """Transfer formats of one of this connector's datasets (from its distributions)."""
    return {"formats": await registry(ctx).get(connector).request("GET", f"/api/v1/datasets/{seg(id)}/formats")}


async def get_dataset_artifact(ctx: Context, id: str, connector: str | None = None) -> Any:
    """The artifact behind a dataset: FILE (stored upload) or EXTERNAL (a URL the connector fetches)."""
    return await registry(ctx).get(connector).request("GET", f"/api/v1/datasets/{seg(id)}/artifact")


async def get_artifacts(ctx: Context, artifact_id: str | None = None, connector: str | None = None) -> Any:
    """Artifact metadata: all artifacts, or only the one with `artifact_id` (the artifact's own id)."""
    path = "/api/v1/artifacts" + (f"/{seg(artifact_id)}" if artifact_id else "")
    return {"artifacts": await registry(ctx).get(connector).request("GET", path)}


def register(mcp: MCPServer) -> None:
    for write in (create_dataset, update_dataset, publish_dataset):
        mcp.add_tool(write, annotations=WRITE)
    for read in (get_dataset_formats, get_dataset_artifact, get_artifacts):
        mcp.add_tool(read, annotations=READ_ONLY)
    register_readers(mcp, "dataset", "datasets", "/api/v1/datasets")
