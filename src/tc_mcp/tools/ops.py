import json
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from tc_mcp.registry import registry
from tc_mcp.tools.common import READ_ONLY, Page, PageNumber, PageSize, page_params, seg

Instant = Annotated[str, Field(description="ISO-8601 instant, e.g. 2026-11-01T00:00:00Z")]

# Details that hold a whole record as JSON text, as it was at the time of the event
SNAPSHOTS = {
    "agreement",
    "offer",
    "decision",
    "contractNegotiation",
    "contractNegotiationFromResponse",
    "transferProcess",
}


def listed_details(details: dict[str, Any]) -> dict[str, Any]:
    """Details without the snapshots, but with the ids that tie the event to its negotiation, transfer or agreement:
    policy evaluations and negotiation requests carry them only inside a snapshot."""
    kept = {k: v for k, v in details.items() if k not in SNAPSHOTS}
    for key in SNAPSHOTS & details.keys():
        try:
            record = json.loads(details[key])
        except (TypeError, ValueError):
            continue
        if not isinstance(record, dict):
            continue
        ids = {"agreementId": record.get("@id")} if key == "agreement" else {}
        ids |= {pid: record.get(pid) for pid in ("consumerPid", "providerPid")}
        for name, value in ids.items():
            if value:
                kept.setdefault(name, value)
    return kept


async def get_dspace_version(ctx: Context, connector: str | None = None) -> Any:
    """Dataspace Protocol versions the connector supports (public endpoint, no login)."""
    return await registry(ctx).get(connector).request("GET", "/.well-known/dspace-version", auth=False)


async def get_dashboard(
    ctx: Context,
    view: Literal["summary", "runtime", "negotiations", "transfers", "events"] = "summary",
    from_: Instant | None = None,
    to: Instant | None = None,
    bucket: Literal["hour", "day"] | None = None,
    connector: str | None = None,
) -> Any:
    """This connector's metrics: negotiation and transfer counts by state, role and format; audit events by type;
    or runtime (CPU, memory, threads, uptime). `summary` combines them.

    `from_`, `to` and `bucket` (time series granularity) apply to the summary and events views only.
    """
    window = {"from": from_, "to": to, "bucket": bucket}
    if view not in ("summary", "events") and any(window.values()):
        raise ToolError(f"from_, to and bucket apply to the summary and events views, not to {view}")
    params = {k: v for k, v in window.items() if v is not None}
    return await registry(ctx).get(connector).request("GET", f"/api/v1/dashboard/{view}", params=params)


async def list_audit_events(
    ctx: Context,
    event_type: Annotated[
        list[str] | None,
        Field(description="Event type codes, e.g. APPLICATION_LOGIN (see trueconnector://{connector}/audit/types)"),
    ] = None,
    consumer_pid: str | None = None,
    page: PageNumber = 0,
    size: PageSize = 20,
    connector: str | None = None,
) -> Page:
    """This connector's audit events, newest first. Results show each event's type by its description ("Login");
    filter by its code (APPLICATION_LOGIN).

    `consumer_pid` keeps one negotiation's or transfer's events (TrueConnector matches details.consumerPid). Every
    transfer event has it; policy evaluations and some negotiation events do not, so list those by event_type.
    `details` leave out the record snapshots (agreement, offer, decision, contractNegotiation, transferProcess) but
    keep their consumerPid, providerPid and agreementId; get_audit_event returns the full event.
    """
    client = registry(ctx).get(connector)
    params = page_params(page, size)
    if consumer_pid:
        params["details.consumerPid"] = consumer_pid
    if event_type:
        # An unknown code is not an error to TrueConnector: it just matches nothing.
        codes = {t["code"] for t in await client.request("GET", "/api/v1/audit/types")}
        if unknown := [t for t in event_type if t not in codes]:
            raise ToolError(
                f"unknown event type codes {unknown}; the codes are listed in trueconnector://"
                f"{client.connector.name}/audit/types, e.g. APPLICATION_LOGIN for 'Login'"
            )
        params["eventType"] = event_type
    listed = await client.request("GET", "/api/v1/audit", params=params)
    for event in listed["items"]:
        if event.get("details"):
            event["details"] = listed_details(event["details"])
    return Page.model_validate(listed)


async def get_audit_event(ctx: Context, id: str, connector: str | None = None) -> Any:
    """One audit event by its `id`."""
    return await registry(ctx).get(connector).request("GET", f"/api/v1/audit/{seg(id)}")


def register(mcp: MCPServer) -> None:
    for read in (get_dspace_version, get_dashboard, list_audit_events, get_audit_event):
        mcp.add_tool(read, annotations=READ_ONLY)
