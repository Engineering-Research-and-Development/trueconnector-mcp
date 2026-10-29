from typing import Any, Literal, cast

from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from tc_mcp.client import Registry

READ_ONLY = ToolAnnotations(read_only_hint=True)
WRITE = ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=False)
DESTRUCTIVE = ToolAnnotations(read_only_hint=False, destructive_hint=True)


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
    constraint: list[Constraint] = []


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
