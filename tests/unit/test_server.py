import json

import httpx2
import mcp
import pytest
from helpers import FIXTURES, TWO_CONNECTORS, FakeConnector

from tc_mcp.config import Settings
from tc_mcp.server import build_server

pytestmark = pytest.mark.anyio


class ClosableTransport(httpx2.MockTransport):
    closed = False

    async def aclose(self):
        self.closed = True


async def test_dspace_version_over_mcp():
    # Credentials are set but there is no login route: the tool must not authenticate.
    fake = FakeConnector({("GET", "/.well-known/dspace-version"): "dspace_version"})
    settings = Settings(
        _env_file=None,
        connectors=[{"name": "a", "base_url": "http://tc", "email": "admin@mail.com", "password": "pw"}],
    )

    transport = ClosableTransport(fake)

    async with mcp.Client(build_server(settings, transport)) as client:
        result = await client.call_tool("get_dspace_version", {})

    assert not result.is_error, result.content
    assert json.loads(result.content[0].text) == FIXTURES["dspace_version"]["body"]
    assert len(fake.requests) == 1
    assert transport.closed  # the lifespan closed the registry on shutdown


async def test_every_tool_declares_its_effect():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t.annotations for t in (await client.list_tools()).tools}

    reads = {"get_dspace_version", "browse_remote_catalog", "get_remote_dataset_formats", "get_own_catalog"}
    reads |= {"get_dashboard", "list_audit_events", "get_audit_event"}
    reads |= {"get_catalog", "get_dataset_formats", "get_dataset_artifact", "get_artifacts"}
    reads |= {
        "list_negotiations",
        "get_negotiation",
        "find_counterpart_negotiation",
        "validate_offer",
        "enforce_agreement",
        "wait_for_state",
        "list_transfers",
        "get_transfer",
        "find_transfer_for_agreement",
        "get_transfer_data_url",
    }
    reads |= {f"list_{kind}" for kind in ("datasets", "distributions", "data_services")}
    reads |= {f"get_{kind}" for kind in ("dataset", "distribution", "data_service")}
    kinds = ("catalog", "data_service", "distribution", "dataset")
    # A repeat would create or send something again.
    writes = {f"create_{kind}" for kind in kinds} | {"publish_dataset", "request_contract", "send_counteroffer"}
    writes |= {"move_transfer_data"}  # a repeat after a failed move starts a new one
    repeatable = {f"update_{kind}" for kind in kinds} | {"request_transfer"}
    repeatable |= {f"{action}_negotiation" for action in ("accept", "agree", "verify", "finalize")}
    repeatable |= {f"{action}_transfer" for action in ("start", "complete", "suspend")}
    deletes = {f"delete_{kind}" for kind in kinds} | {"terminate_negotiation", "terminate_transfer"}
    assert set(tools) == reads | writes | repeatable | deletes
    effect = {name: (a.read_only_hint, a.destructive_hint, a.idempotent_hint) for name, a in tools.items()}
    for name in reads:
        assert effect[name] == (True, None, None), name
    for name in writes:
        assert effect[name] == (False, False, False), name
    for name in repeatable:
        assert effect[name] == (False, False, True), name
    for name in deletes:
        assert effect[name] == (False, True, True), name


async def test_read_only_mode_exposes_only_read_tools():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        reads = {t.name for t in (await client.list_tools()).tools if t.annotations.read_only_hint}

    server = build_server(TWO_CONNECTORS.model_copy(update={"read_only": True}))
    for _ in range(2):  # the server can be started again after its tools were removed
        async with mcp.Client(server) as client:
            tools = {t.name for t in (await client.list_tools()).tools}
            prompts = {p.name for p in (await client.list_prompts()).prompts}
            blocked = await client.call_tool("delete_dataset", {"id": "x"})

        assert tools == reads
        assert prompts == {"diagnose_process"}
        assert blocked.is_error and "Unknown tool: delete_dataset" in blocked.content[0].text
