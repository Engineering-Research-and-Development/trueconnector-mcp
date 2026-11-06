import inspect
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, Field

from tc_mcp.registry import registry
from tc_mcp.tools.common import (
    DESTRUCTIVE,
    READ_ONLY,
    REPEATABLE,
    WRITE,
    Format,
    Page,
    PageNumber,
    PageSize,
    Role,
    check_side,
    page_params,
    seg,
    transition,
)

TransferState = Literal["INITIALIZED", "REQUESTED", "STARTED", "SUSPENDED", "COMPLETED", "TERMINATED"]

# From it.eng.datatransfer.model.TransferState.
STATE_MACHINE = """Transfer states (the consumer requests, the provider starts):
INITIALIZED -> REQUESTED
REQUESTED -> STARTED | TERMINATED
STARTED -> SUSPENDED | COMPLETED | TERMINATED
SUSPENDED -> STARTED | TERMINATED
COMPLETED and TERMINATED are final. Finalizing a negotiation creates an INITIALIZED transfer on each side, with the
negotiation's agreement id as agreementId; `@id` differs per side."""


class TransferStep(BaseModel):
    already_done: bool
    state: TransferState
    transfer: dict[str, Any]


async def list_transfers(
    ctx: Context,
    state: TransferState | None = None,
    role: Role | None = None,
    agreement_id: str | None = None,
    dataset_id: str | None = None,
    format: Format | None = None,
    page: PageNumber = 0,
    size: PageSize = 20,
    connector: str | None = None,
) -> Page:
    """This connector's transfer processes, filtered by exact match."""
    params = page_params(
        page, size, state=state, role=role, agreement_id=agreement_id, dataset_id=dataset_id, format=format
    )
    return Page.model_validate(await registry(ctx).get(connector).request("GET", "/api/v1/transfers", params=params))


async def get_transfer(ctx: Context, id: str, connector: str | None = None) -> Any:
    """One transfer process by this connector's `@id`, including `downloaded` and `downloadInProgress`."""
    return await registry(ctx).get(connector).request("GET", f"/api/v1/transfers/{seg(id)}")


async def find_transfer_for_agreement(
    ctx: Context, agreement_id: str, role: Role | None = None, connector: str | None = None
) -> Any:
    """This connector's transfer process for an agreement (the negotiation's `agreement["@id"]`).

    `role` is only needed when this connector holds both sides of the agreement.
    """
    client = registry(ctx).get(connector)
    params = page_params(0, 100, agreement_id=agreement_id, role=role)
    found = (await client.request("GET", "/api/v1/transfers", params=params))["items"]
    if len(found) == 1:
        return found[0]
    if not found:
        raise ToolError(
            f"connector '{client.connector.name}' has no transfer for agreement {agreement_id}"
            f"{f' with role {role}' if role else ''}: is its negotiation FINALIZED?"
        )
    raise ToolError(
        f"connector '{client.connector.name}' has {len(found)} transfers for agreement {agreement_id}: pass role"
    )


async def request_transfer(
    ctx: Context, transfer_process_id: str, format: Format, connector: str | None = None
) -> TransferStep:
    """Consumer: ask the provider to transfer the data (INITIALIZED → REQUESTED).

    `transfer_process_id` is this connector's (the consumer's) transfer `@id`, from find_transfer_for_agreement.
    `format` must be one the dataset offers (get_remote_dataset_formats). PUSH needs no data address: the consumer
    supplies its own storage.
    """
    client = registry(ctx).get(connector)
    path = f"/api/v1/transfers/{seg(transfer_process_id)}"
    record = await client.request("GET", path)
    check_side(client, record, "consumer", transfer_process_id, "request is a consumer action", "its transfer @id")
    body = {"transferProcessId": transfer_process_id, "format": format}
    already_done, result = await transition(client, path, client.request("POST", "/api/v1/transfers", json=body))
    return TransferStep(already_done=already_done, state=result["state"], transfer=result)


class Move(BaseModel):
    accepted: bool = Field(description="A data move was started now; false when it is already done or running")
    message: str


class DataUrl(BaseModel):
    url: str


async def move_transfer_data(ctx: Context, transfer_process_id: str, connector: str | None = None) -> Move:
    """Start moving the data of a STARTED transfer, in the background (watch `downloaded` on the transfer).

    PULL: call it on the consumer, which fetches the data. PUSH: call it on the provider, which uploads it to the
    consumer's storage. Once the data has arrived the transfer is COMPLETED on both sides.
    """
    reg = registry(ctx)
    client = reg.get(connector)
    path = f"/api/v1/transfers/{seg(transfer_process_id)}"
    record = await client.request("GET", path)
    if record.get("format") not in ("HttpData-PULL", "HttpData-PUSH"):
        raise ToolError(f"transfer {transfer_process_id} has no format yet: the consumer must request_transfer first")
    side: Role = "consumer" if record["format"] == "HttpData-PULL" else "provider"
    # The wrong side is not refused by TrueConnector: PUSH on the consumer is a 500 "Decryption failed", and PULL on
    # the provider is accepted and copies the data into the provider's own storage.
    what = f"{record['format']} data is moved by the {side}"
    check_side(client, record, side, transfer_process_id, what, "its transfer @id")
    if record.get("downloaded"):
        return Move(accepted=False, message="the data is already downloaded: get_transfer_data_url gives its link")
    if record["state"] != "STARTED":
        raise ToolError(f"transfer {transfer_process_id} is {record['state']}: data moves only while STARTED")
    if record.get("downloadInProgress"):
        return Move(accepted=False, message="a data move is already running for this transfer")
    message = await client.request("GET", f"{path}/download")
    reg.moves_started.add((client.connector.name, record["@id"]))
    return Move(accepted=True, message=str(message))


async def get_transfer_data_url(ctx: Context, transfer_process_id: str, connector: str | None = None) -> DataUrl:
    """A time-limited download link to the transferred data, once the transfer is COMPLETED.

    Anyone holding the link can download the data until it expires: share it only with whoever should have it.
    """
    path = f"/api/v1/transfers/{seg(transfer_process_id)}/view"
    return DataUrl(url=await registry(ctx).get(connector).request("GET", path))


# TrueConnector checks the side itself and says so (e.g. "consumer can not transit from REQUESTED to STARTED").
TRANSITIONS = {
    "start": "Provider: start a REQUESTED transfer (→ STARTED). Either side: resume a SUSPENDED one.",
    "complete": "Either side: mark a STARTED transfer as done (→ COMPLETED).",
    "suspend": "Either side: pause a STARTED transfer (→ SUSPENDED); start_transfer resumes it.",
    "terminate": "Either side: end a REQUESTED, STARTED or SUSPENDED transfer (→ TERMINATED). Irreversible.",
}


def register_transition(mcp: MCPServer, action: str, description: str) -> None:
    async def run(ctx: Context, transfer_process_id: str, connector: str | None = None) -> TransferStep:
        client = registry(ctx).get(connector)
        path = f"/api/v1/transfers/{seg(transfer_process_id)}"
        already_done, record = await transition(client, path, client.request("PUT", f"{path}/{action}"))
        return TransferStep(already_done=already_done, state=record["state"], transfer=record)

    mcp.add_tool(
        run,
        name=f"{action}_transfer",
        description=f"{description} A repeated call returns already_done.\n\n{STATE_MACHINE}",
        annotations=DESTRUCTIVE if action == "terminate" else REPEATABLE,
    )


def register(mcp: MCPServer) -> None:
    for read in (list_transfers, get_transfer, find_transfer_for_agreement, get_transfer_data_url):
        mcp.add_tool(
            read, annotations=READ_ONLY, description=f"{inspect.cleandoc(read.__doc__ or '')}\n\n{STATE_MACHINE}"
        )
    for write, annotations in ((request_transfer, REPEATABLE), (move_transfer_data, WRITE)):
        mcp.add_tool(
            write, annotations=annotations, description=f"{inspect.cleandoc(write.__doc__ or '')}\n\n{STATE_MACHINE}"
        )
    for action, description in TRANSITIONS.items():
        register_transition(mcp, action, description)
