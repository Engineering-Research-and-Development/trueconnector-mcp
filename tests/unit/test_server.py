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


async def test_tool_catalog_and_annotations():
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t.annotations for t in (await client.list_tools()).tools}

    reads = {"get_dspace_version", "browse_remote_catalog", "get_remote_dataset_formats", "get_own_catalog"}
    reads |= {"get_catalog", "get_dataset_formats", "get_dataset_artifact", "get_artifacts"}
    reads |= {
        "list_negotiations",
        "get_negotiation",
        "find_counterpart_negotiation",
        "validate_offer",
        "enforce_agreement",
        "wait_for_state",
    }
    reads |= {f"list_{kind}" for kind in ("datasets", "distributions", "data_services")}
    reads |= {f"get_{kind}" for kind in ("dataset", "distribution", "data_service")}
    kinds = ("catalog", "data_service", "distribution", "dataset")
    writes = {f"{verb}_{kind}" for verb in ("create", "update") for kind in kinds} | {"publish_dataset"}
    writes |= {"request_contract", "send_counteroffer"}
    writes |= {f"{action}_negotiation" for action in ("accept", "agree", "verify", "finalize")}
    deletes = {f"delete_{kind}" for kind in kinds} | {"terminate_negotiation"}
    assert set(tools) == reads | writes | deletes
    for name in reads:
        assert tools[name].read_only_hint, name
    for name in writes:
        assert (tools[name].read_only_hint, tools[name].destructive_hint, tools[name].idempotent_hint) == (
            False,
            False,
            False,
        ), name
    for name in deletes:
        assert (tools[name].read_only_hint, tools[name].destructive_hint) == (False, True), name
