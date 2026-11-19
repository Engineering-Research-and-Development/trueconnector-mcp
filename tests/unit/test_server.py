import json
import logging
import re

import httpx2
import mcp
import pytest
from helpers import FIXTURES, TWO_CONNECTORS, FakeConnector, login_response
from mcp.server.mcpserver import MCPServer

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
    deletes = {f"delete_{kind}" for kind in kinds} | {"unpublish_dataset"}
    deletes |= {"terminate_negotiation", "terminate_transfer"}
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


def leaves(schema, path=()):
    """Every (path, value) leaf of a JSON document; an empty object or list counts as a leaf."""
    if isinstance(schema, dict) and schema:
        return {leaf for key, value in schema.items() for leaf in leaves(value, (*path, key))}
    if isinstance(schema, list) and schema:
        return {leaf for i, value in enumerate(schema) for leaf in leaves(value, (*path, i))}
    return {(path, json.dumps(schema))}


async def test_input_schemas_are_listed_as_generated_minus_the_titles():
    server = build_server(TWO_CONNECTORS)
    generated = {t.name: t.input_schema for t in await MCPServer.list_tools(server)}
    async with mcp.Client(server) as client:
        listed = {t.name: t.input_schema for t in (await client.list_tools()).tools}

    assert any(path[-1] == "title" for schema in generated.values() for path, _ in leaves(schema))
    for name, schema in generated.items():
        assert leaves(listed[name]) == {(p, v) for p, v in leaves(schema) if p[-1] != "title"}, name
    # An argument called `title` is a property name, not a generated title.
    assert "title" in listed["publish_dataset"]["properties"]


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


async def test_logs_calls_without_secrets_or_bodies(caplog):
    fake = FakeConnector(
        {
            ("POST", "/api/v1/auth/login"): lambda r: login_response("ACCESS-SECRET", "REFRESH-SECRET"),
            ("POST", "/api/v1/catalogs"): "envelope_data",
            ("POST", "/api/v1/auth/logout"): lambda r: httpx2.Response(200),
        }
    )
    settings = Settings(
        _env_file=None,
        connectors=[{"name": "a", "base_url": "http://tc", "email": "admin@mail.com", "password": "PASSWORD-SECRET"}],
    )

    with caplog.at_level(logging.INFO):
        async with mcp.Client(build_server(settings, httpx2.MockTransport(fake))) as client:
            result = await client.call_tool("create_catalog", {"catalog": {"title": "BODY-SECRET"}})

    assert not result.is_error, result.content
    calls = [r.getMessage().rsplit(" ", 1) for r in caplog.records if r.name == "tc_mcp.client"]
    assert [line for line, _ in calls] == [
        "a POST /api/v1/auth/login 200",
        "a POST /api/v1/catalogs 200",
        "a POST /api/v1/auth/logout 200",
    ]
    assert all(re.fullmatch(r"\d+ms", ms) for _, ms in calls)
    assert not [r for r in caplog.records if r.name.startswith("httpx2")]  # it would log full URLs
    for secret in ("PASSWORD-SECRET", "ACCESS-SECRET", "REFRESH-SECRET", "BODY-SECRET"):
        assert secret not in caplog.text, secret
