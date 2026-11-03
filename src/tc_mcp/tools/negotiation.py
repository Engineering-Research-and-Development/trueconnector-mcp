import inspect
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, Field

from tc_mcp.client import AlreadyInState, Rejected
from tc_mcp.tools import (
    DESTRUCTIVE,
    READ_ONLY,
    WRITE,
    Page,
    PageNumber,
    PageSize,
    Permission,
    Role,
    page_params,
    registry,
    seg,
)

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


def pick_offer(catalog: dict[str, Any], dataset_id: str, offer_id: str | None, owner: str) -> dict[str, Any]:
    """One of a catalog dataset's offers, ready to send: `target` = the dataset, `assigner` = the catalog owner."""
    dataset = next((d for d in catalog.get("dataset") or [] if d.get("@id") == dataset_id), None)
    if dataset is None:
        raise ToolError(f"dataset {dataset_id} is not in the catalog of {owner}")
    offers: list[dict[str, Any]] = dataset.get("hasPolicy") or []
    ids = [o.get("@id") for o in offers]
    if offer_id is None and len(offers) != 1:
        raise ToolError(f"dataset {dataset_id} has {len(offers)} offers: pass offer_id, one of {ids}")
    offer = next((o for o in offers if offer_id in (None, o.get("@id"))), None)
    if offer is None:
        raise ToolError(f"offer {offer_id} is not one of dataset {dataset_id}'s offers {ids}")
    return offer | {"target": dataset_id, "assigner": catalog.get("participantId")}


async def request_contract(
    ctx: Context, peer: str, dataset_id: str, offer_id: str | None = None, connector: str | None = None
) -> Any:
    """Consumer: start a negotiation for one of a provider's datasets, accepting one of its offers as published.

    `peer` is the provider: a configured connector name or its DSP protocol URL. `offer_id` is one of the dataset's
    offers from browse_remote_catalog; it may be omitted when the dataset has exactly one. Returns this connector's
    record (REQUESTED; with automatic negotiation the connectors reach FINALIZED within seconds).
    """
    reg = registry(ctx)
    client, forward_to = reg.get(connector), reg.forward_to(peer)
    catalog = await client.request("POST", "/api/v1/proxy/catalogs", json={"Forward-To": forward_to})
    offer = pick_offer(catalog, dataset_id, offer_id, forward_to)
    return await client.request("POST", "/api/v1/negotiations/request", json={"Forward-To": forward_to, "offer": offer})


async def send_counteroffer(
    ctx: Context,
    negotiation_id: str,
    permissions: Annotated[list[Permission], Field(min_length=1, description="All go into the one counteroffer")],
    connector: str | None = None,
) -> Any:
    """Answer the other side with different usage terms; only the permissions change.

    On a provider record: REQUESTED → OFFERED. On a consumer record: OFFERED → REQUESTED. The offer id, target and
    parties are taken from the negotiation, as TrueConnector requires.
    """
    client = registry(ctx).get(connector)
    negotiation = await client.request("GET", f"/api/v1/negotiations/{seg(negotiation_id)}")
    stored = negotiation["offer"]
    provider = negotiation["role"] == "provider"
    # The counteroffer replaces the stored offer, and agree builds the agreement from it: it needs an assignee.
    own = client.connector.protocol_url
    fallback = negotiation.get("callbackAddress") if provider else own and f"{own.rstrip('/')}/consumer"
    assignee = stored.get("assignee") or fallback
    if not assignee:
        source = "callbackAddress" if provider else f"protocol_url configured for connector '{client.connector.name}'"
        raise ToolError(f"negotiation {negotiation_id} has no assignee to keep and no {source} to derive one from")
    offer = {
        "@id": stored["originalId"],
        "target": stored["target"],
        "assigner": stored["assigner"],
        "assignee": assignee,
        "permission": [p.model_dump() for p in permissions],
    }
    action = "offer" if provider else "request"
    return await client.request("PUT", f"/api/v1/negotiations/{seg(negotiation_id)}/{action}", json=offer)


class Transition(BaseModel):
    already_done: bool = Field(description="The negotiation was already in this state; nothing changed")
    state: NegotiationState
    negotiation: dict[str, Any]


# action: (the side that performs it, or None for either; description)
TRANSITIONS: dict[str, tuple[Role | None, str]] = {
    "accept": ("consumer", "Consumer: accept the provider's offer (OFFERED → ACCEPTED)."),
    "agree": ("provider", "Provider: agree (REQUESTED or ACCEPTED → AGREED); creates the agreement."),
    "verify": ("consumer", "Consumer: verify the provider's agreement (AGREED → VERIFIED)."),
    "finalize": (
        "provider",
        "Provider: finalize (VERIFIED → FINALIZED); the agreement can then be used for transfers.",
    ),
    "terminate": (None, "Either side: end the negotiation for both sides (→ TERMINATED). Irreversible."),
}


def register_transition(mcp: MCPServer, action: str, role: Role | None, description: str) -> None:
    async def transition(ctx: Context, negotiation_id: str, connector: str | None = None) -> Transition:
        client = registry(ctx).get(connector)
        path = f"/api/v1/negotiations/{seg(negotiation_id)}"
        record = await client.request("GET", path)
        # A wrong-side call fails misleadingly (agree on a consumer record is a 500 about a missing assignee).
        if role is not None and record["role"] != role:
            raise ToolError(
                f"{action} is a {role} action, but {negotiation_id} is the {record['role']} record on connector "
                f"'{client.connector.name}': call it on the {role}'s connector with find_counterpart_negotiation's @id"
            )
        already_done, result = False, None
        try:
            result = await client.request("PUT", f"{path}/{action}")
        except AlreadyInState:
            already_done = True
        if not (isinstance(result, dict) and "state" in result):
            result = await client.request("GET", path)
        return Transition(already_done=already_done, state=result["state"], negotiation=result)

    mcp.add_tool(
        transition,
        name=f"{action}_negotiation",
        description=f"{description} A repeated call returns already_done.\n\n{STATE_MACHINE}",
        annotations=DESTRUCTIVE if action == "terminate" else WRITE,
    )


def register(mcp: MCPServer) -> None:
    for read in (list_negotiations, get_negotiation, find_counterpart_negotiation):
        mcp.add_tool(
            read, annotations=READ_ONLY, description=f"{inspect.cleandoc(read.__doc__ or '')}\n\n{STATE_MACHINE}"
        )
    for check in (validate_offer, enforce_agreement):
        mcp.add_tool(check, annotations=READ_ONLY)
    for write in (request_contract, send_counteroffer):
        mcp.add_tool(
            write, annotations=WRITE, description=f"{inspect.cleandoc(write.__doc__ or '')}\n\n{STATE_MACHINE}"
        )
    for action, (role, description) in TRANSITIONS.items():
        register_transition(mcp, action, role, description)
