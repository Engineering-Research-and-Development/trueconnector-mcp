import inspect
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel

from tc_mcp.client import Rejected
from tc_mcp.tools import READ_ONLY, Page, PageNumber, PageSize, Role, page_params, registry, seg

NegotiationState = Literal["REQUESTED", "OFFERED", "ACCEPTED", "AGREED", "VERIFIED", "FINALIZED", "TERMINATED"]

# From it.eng.negotiation.model.ContractNegotiationState.
STATE_MACHINE = """Negotiation states (consumer actions: request, counteroffer, accept, verify; provider actions: offer,
counteroffer, agree, finalize; either side can terminate):
REQUESTED -> OFFERED | AGREED | TERMINATED
OFFERED -> REQUESTED | ACCEPTED | TERMINATED
ACCEPTED -> AGREED | TERMINATED
AGREED -> VERIFIED | TERMINATED
VERIFIED -> FINALIZED | TERMINATED
FINALIZED and TERMINATED are final. Each side keeps its own record (role consumer or provider) with the same
consumerPid; `@id` differs per side."""


async def list_negotiations(
    ctx: Context,
    state: NegotiationState | None = None,
    role: Role | None = None,
    consumer_pid: str | None = None,
    provider_pid: str | None = None,
    assigner: str | None = None,
    page: PageNumber = 0,
    size: PageSize = 20,
    connector: str | None = None,
) -> Page:
    """This connector's negotiations, newest first, filtered by exact match."""
    params = page_params(
        page, size, state=state, role=role, consumer_pid=consumer_pid, provider_pid=provider_pid, assigner=assigner
    )
    return Page.model_validate(await registry(ctx).get(connector).request("GET", "/api/v1/negotiations", params=params))


async def get_negotiation(ctx: Context, id: str, connector: str | None = None) -> Any:
    """One negotiation by this connector's `@id`, with its current offer and, once agreed, the agreement."""
    return await registry(ctx).get(connector).request("GET", f"/api/v1/negotiations/{seg(id)}")


async def find_counterpart_negotiation(ctx: Context, id: str, peer_connector: str, connector: str | None = None) -> Any:
    """The other side's record of a negotiation: same consumerPid, opposite role, on `peer_connector`.

    Use it to act on the provider side of a negotiation this connector requested (or the reverse). Both connectors
    must be configured here.
    """
    reg = registry(ctx)
    mine = await reg.get(connector).request("GET", f"/api/v1/negotiations/{seg(id)}")
    params = page_params(0, 100, consumer_pid=mine["consumerPid"])
    theirs = await reg.get(peer_connector).request("GET", "/api/v1/negotiations", params=params)
    # One connector can hold both roles for the same consumerPid (talking to itself), so skip our own role.
    for record in theirs["items"]:
        if record.get("role") != mine.get("role"):
            return record
    raise ToolError(
        f"{peer_connector} has no counterpart for consumerPid {mine['consumerPid']} (role ≠ {mine.get('role')})"
    )


class OfferValidation(BaseModel):
    valid: bool
    message: str


class Enforcement(BaseModel):
    allowed: bool
    message: str


async def validate_offer(ctx: Context, offer: dict[str, Any], connector: str | None = None) -> OfferValidation:
    """Check an ODRL offer against this connector's catalog: run it on the provider that published the offer.

    `offer` is one of a dataset's `hasPolicy` offers with `target` (the dataset id) and `assigner` (the provider's
    participantId) set. `valid: false` carries the connector's reason.
    """
    try:
        message = await registry(ctx).get(connector).request("POST", "/api/v1/offers/validate", json=offer)
    except Rejected as e:
        return OfferValidation(valid=False, message=e.reason)
    return OfferValidation(valid=True, message=message)


async def enforce_agreement(ctx: Context, agreement_id: str, connector: str | None = None) -> Enforcement:
    """Evaluate an agreement's usage policy now (e.g. remaining COUNT, DATE_TIME window); changes nothing.

    `agreement_id` is the negotiation's `agreement["@id"]`; both sides hold it. `allowed: false` also when the
    negotiation is not FINALIZED.
    """
    try:
        message = await registry(ctx).get(connector).request("POST", f"/api/v1/agreements/{seg(agreement_id)}/enforce")
    except Rejected as e:
        return Enforcement(allowed=False, message=e.reason)
    return Enforcement(allowed=True, message=message)


def register(mcp: MCPServer) -> None:
    for read in (list_negotiations, get_negotiation, find_counterpart_negotiation):
        mcp.add_tool(
            read, annotations=READ_ONLY, description=f"{inspect.cleandoc(read.__doc__ or '')}\n\n{STATE_MACHINE}"
        )
    for check in (validate_offer, enforce_agreement):
        mcp.add_tool(check, annotations=READ_ONLY)
