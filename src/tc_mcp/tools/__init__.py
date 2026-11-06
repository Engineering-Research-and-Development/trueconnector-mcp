from collections.abc import Awaitable
from typing import Annotated, Any, Literal, cast
from urllib.parse import quote

import httpx2
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from tc_mcp.client import AlreadyInState, TCClient
from tc_mcp.config import Settings

READ_ONLY = ToolAnnotations(read_only_hint=True)
WRITE = ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=False)
# A repeat changes nothing: updates set the same values, lifecycle steps return already_done.
REPEATABLE = ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=True)
DESTRUCTIVE = ToolAnnotations(read_only_hint=False, destructive_hint=True, idempotent_hint=True)

Role = Literal["consumer", "provider"]
Format = Literal["HttpData-PULL", "HttpData-PUSH"]
PageNumber = Annotated[int, Field(ge=0, description="0-based page number")]
PageSize = Annotated[int, Field(ge=1, le=100)]


class PageInfo(BaseModel):
    size: int
    totalElements: int
    totalPages: int
    number: int


class Page(BaseModel):
    """One page of a paged list; filters are exact matches on the record's fields."""

    items: list[dict[str, Any]]
    page: PageInfo


def page_params(page: int, size: int, **filters: str | None) -> dict[str, Any]:
    """Query parameters for a paged list: snake_case filters become the API's camelCase field names."""
    camel = {"".join(w.capitalize() if i else w for i, w in enumerate(k.split("_"))): v for k, v in filters.items()}
    return {"page": page, "size": size, **{k: v for k, v in camel.items() if v is not None}}


class Constraint(BaseModel):
    """One ODRL constraint. Values are the ones TrueConnector's catalog model accepts (it.eng.catalog.model)."""

    leftOperand: Literal["COUNT", "DATE_TIME"] = Field(description="COUNT: max accesses; DATE_TIME: time limit")
    operator: Literal[
        "EQ",
        "NEQ",
        "GT",
        "GTEQ",
        "LT",
        "LTEQ",
        "IS_A",
        "HAS_PART",
        "IS_PART_OF",
        "IS_ALL_OF",
        "IS_ANY_OF",
        "IS_NONE_OF",
    ]
    rightOperand: str = Field(description='e.g. "5" for COUNT, "2030-12-31T23:59:59Z" for DATE_TIME')


class Permission(BaseModel):
    action: Literal["USE", "ANONYMIZE"] = "USE"
    # TrueConnector accepts and negotiates an unconstrained permission, but the consumer's download then always
    # fails ("Policy check returned an invalid response"), so the data could never be obtained.
    constraint: list[Constraint] = Field(min_length=1)


async def transition(client: TCClient, path: str, step: Awaitable[Any]) -> tuple[bool, dict[str, Any]]:
    """Await the lifecycle request `step`; a repeat (already in that state) is not an error.

    Returns (already_done, the record at `path`).
    """
    already_done, result = False, None
    try:
        result = await step
    except AlreadyInState:
        already_done = True
    if not (isinstance(result, dict) and "state" in result):  # a repeat, or an answer without the record
        result = await client.request("GET", path)
    return already_done, result


def check_side(client: TCClient, record: dict[str, Any], side: Role, id: str, what: str, how: str) -> None:
    """Refuse `record` unless it is `side`'s, naming the connector to call instead."""
    if record["role"] != side:
        raise ToolError(
            f"{what}, but {id} is the {record['role']} record on connector '{client.connector.name}': "
            f"call it on the {side}'s connector with {how}"
        )


class Registry:
    """One TCClient per configured connector; `connector` may be omitted only when there is exactly one."""

    def __init__(self, settings: Settings, transport: httpx2.AsyncBaseTransport | None = None):
        self._clients = {
            c.name: TCClient(c, settings.timeout_s, transport, max_concurrency=settings.max_concurrency)
            for c in settings.connectors
        }
        self.moves_started: set[tuple[str, str]] = set()
        # Downloads dataset sources for store="copy"; never sends connector credentials.
        self.http = httpx2.AsyncClient(timeout=settings.timeout_s, transport=transport, follow_redirects=True)
        self.max_copy_bytes = int(settings.max_copy_mb * 1_000_000)

    def get(self, name: str | None = None) -> TCClient:
        if name is None:
            if len(self._clients) > 1:  # a silent default could act on the wrong side of an exchange
                raise ToolError(f"Several connectors configured ({', '.join(self._clients)}): pass connector")
            return next(iter(self._clients.values()))
        if name not in self._clients:
            raise ToolError(f"Unknown connector '{name}'. Configured: {', '.join(self._clients)}")
        return self._clients[name]

    def forward_to(self, peer: str) -> str:
        """The Forward-To value for `peer`: a configured connector's protocol_url, or an http(s) URL as given."""
        if peer in self._clients:
            url = self._clients[peer].connector.protocol_url
            if url is None:
                raise ToolError(f"Connector '{peer}' has no protocol_url configured; pass its DSP protocol URL instead")
            return url
        if peer.startswith(("http://", "https://")):
            return peer
        raise ToolError(
            f"Unknown peer '{peer}': use a configured connector name ({', '.join(self._clients)}) "
            "or the peer's DSP protocol URL, e.g. http://connector-b:8080/connector-b"
        )

    async def aclose(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        await self.http.aclose()


def registry(ctx: Context) -> Registry:
    """The connector registry the server's lifespan created."""
    return cast(Registry, ctx.request_context.lifespan_context)


def lang(text: str) -> list[dict[str, str]]:
    """TrueConnector's multilingual text shape."""
    return [{"value": text, "language": "en"}]


def text(value: Any) -> str | None:
    """First value of a multilingual text list, or the value itself if it is a plain string."""
    if isinstance(value, list):
        return next((v.get("value") for v in value if isinstance(v, dict)), None)
    return value if isinstance(value, str) else None


def seg(value: str) -> str:
    """An ID as one URL path segment: `/`, `?`, `#` and spaces are quoted, the colons of `urn:uuid:` are kept."""
    return quote(value, safe=":")


def fields(
    title: str | None = None, description: str | None = None, keywords: list[str] | None = None
) -> dict[str, Any]:
    """The given descriptive fields in TrueConnector's shape; fields left as None are omitted."""
    given = {"title": title, "description": description and lang(description), "keyword": keywords}
    return {k: v for k, v in given.items() if v is not None}


def register_readers(mcp: MCPServer, singular: str, plural: str, path: str) -> None:
    """list_/get_/delete_ tools for one resource kind."""
    label = singular.replace("_", " ")

    async def list_all(ctx: Context, connector: str | None = None) -> Any:
        return {plural: await registry(ctx).get(connector).request("GET", path)}

    async def get_one(ctx: Context, id: str, connector: str | None = None) -> Any:
        return await registry(ctx).get(connector).request("GET", f"{path}/{seg(id)}")

    async def delete_one(ctx: Context, id: str, connector: str | None = None) -> Any:
        return await registry(ctx).get(connector).request("DELETE", f"{path}/{seg(id)}")

    mcp.add_tool(list_all, name=f"list_{plural}", description=f"All {label}s on this connector.", annotations=READ_ONLY)
    mcp.add_tool(get_one, name=f"get_{singular}", description=f"One {label} by ID.", annotations=READ_ONLY)
    mcp.add_tool(delete_one, name=f"delete_{singular}", description=f"Delete a {label}.", annotations=DESTRUCTIVE)
