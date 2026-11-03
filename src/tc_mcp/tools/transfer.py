import inspect
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel

from tc_mcp.client import AlreadyInState
from tc_mcp.tools import READ_ONLY, WRITE, Page, PageNumber, PageSize, Role, page_params, registry, seg
from tc_mcp.tools.services import Format

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
    if record["role"] != "consumer":
        raise ToolError(
            f"request is a consumer action, but {transfer_process_id} is the {record['role']} record on connector "
            f"'{client.connector.name}': call it on the consumer's connector with its transfer @id"
        )
    body = {"transferProcessId": transfer_process_id, "format": format}
    try:
        result = await client.request("POST", "/api/v1/transfers", json=body)
        return TransferStep(already_done=False, state=result["state"], transfer=result)
    except AlreadyInState:
        record = await client.request("GET", path)
        return TransferStep(already_done=True, state=record["state"], transfer=record)


def register(mcp: MCPServer) -> None:
    for read in (list_transfers, get_transfer, find_transfer_for_agreement):
        mcp.add_tool(
            read, annotations=READ_ONLY, description=f"{inspect.cleandoc(read.__doc__ or '')}\n\n{STATE_MACHINE}"
        )
    mcp.add_tool(
        request_transfer,
        annotations=WRITE,
        description=f"{inspect.cleandoc(request_transfer.__doc__ or '')}\n\n{STATE_MACHINE}",
    )
