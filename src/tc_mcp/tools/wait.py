from typing import Annotated, Any, Literal

import anyio
from mcp.server.mcpserver import Context, MCPServer
from pydantic import BaseModel, Field

from tc_mcp.tools import READ_ONLY, registry, seg
from tc_mcp.tools.negotiation import NegotiationState

FINAL = {"FINALIZED", "TERMINATED"}


class Wait(BaseModel):
    outcome: Literal["reached", "final", "timeout"] = Field(
        description="reached: one of `states`; final: stopped at a final state not asked for; timeout: neither in time"
    )
    state: str
    object: dict[str, Any]


async def wait_for_state(
    ctx: Context,
    kind: Literal["negotiation"],
    id: str,
    states: Annotated[list[NegotiationState], Field(min_length=1)],
    timeout_s: Annotated[float, Field(gt=0, le=120)] = 30,
    interval_s: Annotated[float, Field(ge=0.5, le=30)] = 2,
    connector: str | None = None,
) -> Wait:
    """Poll a negotiation on this connector until it is in one of `states`, reaches a final state
    (FINALIZED, TERMINATED), or `timeout_s` runs out. Reports progress while it waits; changes nothing."""
    client = registry(ctx).get(connector)
    path = f"/api/v1/negotiations/{seg(id)}"
    deadline = anyio.current_time() + timeout_s
    while True:
        record = await client.request("GET", path)
        state = record["state"]
        if state in states:
            return Wait(outcome="reached", state=state, object=record)
        if state in FINAL:
            return Wait(outcome="final", state=state, object=record)
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            return Wait(outcome="timeout", state=state, object=record)
        await ctx.report_progress(timeout_s - remaining, timeout_s, f"{kind} {id} is {state}")
        await anyio.sleep(min(interval_s, remaining))


def register(mcp: MCPServer) -> None:
    mcp.add_tool(wait_for_state, annotations=READ_ONLY)
