from typing import Annotated, Any, Literal, get_args

import anyio
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, Field

from tc_mcp.registry import registry
from tc_mcp.tools.common import READ_ONLY, seg
from tc_mcp.tools.negotiation import NegotiationState
from tc_mcp.tools.transfer import TransferState, data_move

# kind: (path, the states it can be waited for, final states)
KINDS = {
    "negotiation": ("/api/v1/negotiations", set(get_args(NegotiationState)), {"FINALIZED", "TERMINATED"}),
    "transfer": ("/api/v1/transfers", {*get_args(TransferState), "DOWNLOADED"}, {"COMPLETED", "TERMINATED"}),
}


def failed_move(connector: str, consumer_pid: str) -> str:
    return (
        "The data move ended without data, and TrueConnector reports no error on the transfer. The reason is in "
        f'the audit log: list_audit_events(event_type=["TRANSFER_FAILED"], connector="{connector}"), the event '
        f'whose details.consumerPid is "{consumer_pid}", in details.errorMessage. Usual causes: the dataset links '
        "its data (EXTERNAL artifact), which current TrueConnector builds cannot transfer; the source is "
        "unreachable; or the storage endpoint is unreachable from the other connector. The diagnose_process prompt "
        "goes through it; move_transfer_data can be retried once the cause is fixed."
    )


class Wait(BaseModel):
    outcome: Literal["reached", "final", "failed", "timeout"] = Field(
        description="reached: one of `states`; final: stopped at a final state not asked for; failed: a data move "
        "ended without data (see hint); timeout: none of these in time"
    )
    state: str
    object: dict[str, Any]
    hint: str | None = None


async def wait_for_state(
    ctx: Context,
    kind: Literal["negotiation", "transfer"],
    id: str,
    states: Annotated[list[NegotiationState | TransferState | Literal["DOWNLOADED"]], Field(min_length=1)],
    timeout_s: Annotated[float, Field(gt=0, le=120)] = 30,
    interval_s: Annotated[float, Field(ge=0.5, le=30)] = 2,
    connector: str | None = None,
) -> Wait:
    """Poll a negotiation or transfer on this connector until it is in one of `states`, reaches a final state
    (negotiation: FINALIZED, TERMINATED; transfer: COMPLETED, TERMINATED), or `timeout_s` runs out.

    For a transfer, DOWNLOADED means its data has arrived. Wait on the side that moves the data (PULL: consumer,
    PUSH: provider) to have a silently failed move reported as `failed`. Reports progress; changes nothing.
    """
    path, valid, final = KINDS[kind]
    if wrong := [s for s in states if s not in valid]:
        raise ToolError(f"{wrong} are not {kind} states; use {sorted(valid)}")
    reg = registry(ctx)
    client = reg.get(connector)
    deadline = anyio.current_time() + timeout_s
    seen_running = False
    while True:
        record = await client.request("GET", f"{path}/{seg(id)}")
        state = record["state"]
        moved = seen_running or (client.connector.name, record["@id"]) in reg.moves_started
        move = data_move(record, moved) if kind == "transfer" else None
        if {state, *(["DOWNLOADED"] if move == "arrived" else [])} & set(states):
            return Wait(outcome="reached", state=state, object=record)
        if state in final:
            return Wait(outcome="final", state=state, object=record)
        if move == "moving":
            seen_running = True
        elif move == "failed":
            hint = failed_move(client.connector.name, record["consumerPid"])
            return Wait(outcome="failed", state=state, object=record, hint=hint)
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            return Wait(outcome="timeout", state=state, object=record)
        await ctx.report_progress(timeout_s - remaining, timeout_s, f"{kind} {id} is {state}")
        await anyio.sleep(min(interval_s, remaining))


def register(mcp: MCPServer) -> None:
    mcp.add_tool(wait_for_state, annotations=READ_ONLY)
