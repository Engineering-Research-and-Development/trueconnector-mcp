from typing import Annotated, Any, Literal, cast
from urllib.parse import quote

from mcp.server.mcpserver import Context, MCPServer
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from tc_mcp.client import Registry

READ_ONLY = ToolAnnotations(read_only_hint=True)
WRITE = ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=False)
DESTRUCTIVE = ToolAnnotations(read_only_hint=False, destructive_hint=True)

Role = Literal["consumer", "provider"]
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
