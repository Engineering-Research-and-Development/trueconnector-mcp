import contextlib
import json
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from urllib.parse import unquote, urlsplit

import httpx2
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, Field

from tc_mcp.client import TCClient
from tc_mcp.registry import Registry, registry
from tc_mcp.tools.common import (
    DESTRUCTIVE,
    READ_ONLY,
    REPEATABLE,
    WRITE,
    Format,
    Permission,
    fields,
    register_readers,
    seg,
)

HttpUrl = Annotated[str, Field(pattern=r"^https?://\S+$")]
DistributionIds = Annotated[list[str], Field(min_length=1)]
Offers = Annotated[
    list[Permission], Field(min_length=1, description="Each permission becomes one offer a consumer can pick")
]
Store = Annotated[
    Literal["copy", "link"],
    Field(
        description="copy: this server downloads `external_url` once (up to TC_MAX_COPY_MB) and uploads it as a "
        "FILE, a snapshot consumers can transfer. link: only the URL is kept and the connector fetches it at "
        "transfer time (EXTERNAL); current TrueConnector builds fail to transfer EXTERNAL data, so use it only "
        "with a connector known to support it."
    ),
]


async def _copy(reg: Registry, url: str, authorization: str | None) -> tuple[str, bytes, str]:
    """Download `url` as a multipart file: (filename, content, content type)."""
    headers = {"Authorization": authorization} if authorization else {}
    content = bytearray()
    try:
        async with reg.http.stream("GET", url, headers=headers) as response:
            if not response.is_success:
                raise ToolError(f"could not copy {url}: HTTP {response.status_code}")
            async for chunk in response.aiter_bytes():
                content += chunk
                if len(content) > reg.max_copy_bytes:
                    raise ToolError(
                        f"could not copy {url}: larger than {reg.max_copy_bytes / 1_000_000:g} MB (TC_MAX_COPY_MB)"
                    )
    except httpx2.HTTPError as e:
        raise ToolError(f"could not copy {url}: {e!r}") from e
    name = PurePosixPath(unquote(urlsplit(url).path)).name or "data"
    return name, bytes(content), response.headers.get("content-type", "application/octet-stream").split(";")[0]


async def _artifact(reg: Registry, url: str, authorization: str | None, store: str) -> dict[str, Any]:
    """The multipart fields for the data: a FILE copied from `url`, or an EXTERNAL link to it."""
    if store == "copy":
        return {"file": await _copy(reg, url, authorization)}
    return {"url": (None, url)} | ({"authorization": (None, authorization)} if authorization else {})


def _dataset_parts(dataset: dict[str, Any], artifact: dict[str, Any] | None = None) -> dict[str, Any]:
    """The multipart form for POST/PUT /api/v1/datasets (a JSON body gives 500)."""
    return {"dataset": (None, json.dumps(dataset), "application/json"), **(artifact or {})}


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
    store: Store = "copy",
    keywords: list[str] | None = None,
    connector: str | None = None,
) -> Any:
    """Publish a dataset in this connector's catalog (it is linked automatically).

    The data comes from `external_url`, fetched with `external_authorization` as the Authorization header if
    given; `store` decides whether it is copied now or fetched by the connector at transfer time.
    """
    reg = registry(ctx)
    client = reg.get(connector)
    artifact = await _artifact(reg, external_url, external_authorization, store)
    dataset = {
        **fields(title, description, keywords),
        "distribution": await _distributions(client, distribution_ids),
        "hasPolicy": _offers(offers),
    }
    return await client.request("POST", "/api/v1/datasets", files=_dataset_parts(dataset, artifact))


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
    store: Store = "copy",
    connector: str | None = None,
) -> Any:
    """Change the given fields of a dataset; the others are kept.

    New `offers` replace all current offers and get new IDs. A new `external_url` replaces the data, stored as
    `store` says; re-sending the same URL with store="copy" refreshes the copy.
    """
    # Dataset PUT replaces the whole object, so merge onto the current one.
    reg = registry(ctx)
    client = reg.get(connector)
    artifact = await _artifact(reg, external_url, external_authorization, store) if external_url else None
    path = f"/api/v1/datasets/{seg(id)}"
    dataset = {**await client.request("GET", path), **fields(title, description, keywords)}
    if distribution_ids:
        dataset["distribution"] = await _distributions(client, distribution_ids)
    if offers:
        dataset["hasPolicy"] = _offers(offers)
    return await client.request("PUT", path, files=_dataset_parts(dataset, artifact))


async def publish_dataset(
    ctx: Context,
    title: str,
    description: str,
    offers: Offers,
    external_url: HttpUrl,
    format: Format = "HttpData-PULL",
    external_authorization: str | None = None,
    store: Store = "copy",
    keywords: list[str] | None = None,
    connector: str | None = None,
) -> Any:
    """Publish data from `external_url` in one call: data service, distribution and dataset.

    Ask the user for the usage policy (`offers`) rather than inventing one. The data is fetched first (with
    store="copy"), so an unreachable source creates nothing. If a later step fails, what this call created so
    far is deleted again, so nothing half-published is left behind. Returns the dataset.
    """
    reg = registry(ctx)
    client = reg.get(connector)
    artifact = await _artifact(reg, external_url, external_authorization, store)
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
        return await client.request("POST", "/api/v1/datasets", files=_dataset_parts(dataset, artifact))
    except ToolError as e:
        for path in reversed(created):
            with contextlib.suppress(ToolError):
                await client.request("DELETE", path)
        raise ToolError(f"{e} (publish_dataset deleted the {len(created)} object(s) it had created)") from e


def _live(d: dict[str, Any]) -> list[dict[str, Any]]:
    return [x for x in d.get("distribution") or [] if x]  # a deleted distribution shows up as null


class Part(BaseModel):
    kind: Literal["dataset", "distribution", "data_service"]
    id: str
    reason: str | None = Field(default=None, description="Why it was kept")


class Unpublished(BaseModel):
    deleted: list[Part]
    kept: list[Part]


async def unpublish_dataset(ctx: Context, id: str, connector: str | None = None) -> Unpublished:
    """Delete a dataset, its stored data, and its distributions and their data services, keeping each one another
    dataset or distribution still uses. Irreversible.

    The undo of publish_dataset. delete_dataset alone leaves the distributions and data services behind, and
    deleting a shared one by hand breaks the dataset that still uses it: TrueConnector does not refuse it.
    """
    client = registry(ctx).get(connector)
    dataset = await client.request("GET", f"/api/v1/datasets/{seg(id)}")
    datasets = await client.request("GET", "/api/v1/datasets")
    # Each other dataset's id: the ids of the distributions it uses.
    others = {d["@id"]: {x["@id"] for x in _live(d)} for d in datasets if d["@id"] != dataset["@id"]}
    distributions = _live(dataset)
    result = Unpublished(deleted=[], kept=[])

    async def delete(kind: Literal["distribution", "data_service"], path: str, part_id: str, users: list[str]) -> None:
        if users:
            result.kept.append(Part(kind=kind, id=part_id, reason=f"still used by {', '.join(users)}"))
        else:
            await client.request("DELETE", f"{path}/{seg(part_id)}")
            result.deleted.append(Part(kind=kind, id=part_id))

    try:
        await client.request("DELETE", f"/api/v1/datasets/{seg(id)}")
        result.deleted.append(Part(kind="dataset", id=dataset["@id"]))
        for dist in distributions:
            users = [f"dataset {other}" for other, ids in others.items() if dist["@id"] in ids]
            await delete("distribution", "/api/v1/distributions", dist["@id"], users)
        services = list(dict.fromkeys(d["accessService"]["@id"] for d in distributions if d.get("accessService")))
        remaining = await client.request("GET", "/api/v1/distributions") if services else []
        for service in services:
            users = [
                f"distribution {d['@id']}" for d in remaining if (d.get("accessService") or {}).get("@id") == service
            ]
            await delete("data_service", "/api/v1/dataservices", service, users)
    except ToolError as e:
        done = ", ".join(f"{p.kind} {p.id}" for p in result.deleted) or "nothing"
        raise ToolError(f"{e} (unpublish_dataset had already deleted: {done})") from e
    return result


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
    for write in (create_dataset, publish_dataset):
        mcp.add_tool(write, annotations=WRITE)
    mcp.add_tool(update_dataset, annotations=REPEATABLE)
    mcp.add_tool(unpublish_dataset, annotations=DESTRUCTIVE)
    for read in (get_dataset_formats, get_dataset_artifact, get_artifacts):
        mcp.add_tool(read, annotations=READ_ONLY)
    register_readers(mcp, "dataset", "datasets", "/api/v1/datasets")
