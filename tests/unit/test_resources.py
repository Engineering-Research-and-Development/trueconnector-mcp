import json
import re

import httpx2
import mcp
import pytest
from helpers import FIXTURES, FakeConnector

from tc_mcp.config import Settings
from tc_mcp.prompts import FAILURE_EVENTS
from tc_mcp.server import build_server
from tc_mcp.tools import negotiation, transfer

pytestmark = pytest.mark.anyio

SETTINGS = Settings(
    _env_file=None,
    connectors=[
        {
            "name": "a",
            "base_url": "http://tc-a",
            "protocol_url": "http://connector-a:8080/connector-a",
            "token": "tok-SECRET",
        },
        {"name": "b", "base_url": "http://tc-b", "email": "admin@mail.com", "password": "pw-SECRET"},
        {"name": "c", "base_url": "http://tc-c"},
    ],
)


async def test_resources():
    fake = FakeConnector(
        {
            ("GET", "/api/v1/catalogs"): "envelope_data",
            ("GET", "/api/v1/datasets/urn:uuid:d"): "dataset",
            ("GET", "/api/v1/datasets/missing"): "error_400",
            ("GET", "/api/v1/negotiations/urn:uuid:n"): "envelope_data",
            ("GET", "/api/v1/transfers/urn:uuid:t"): "dataset",
            ("GET", "/api/v1/audit/types"): "audit_types",
        }
    )
    async with mcp.Client(build_server(SETTINGS, httpx2.MockTransport(fake))) as client:
        assert [t.uri_template for t in (await client.list_resource_templates()).resource_templates] == [
            "trueconnector://{connector}/catalog",
            "trueconnector://{connector}/datasets/{id}",
            "trueconnector://{connector}/negotiations/{id}",
            "trueconnector://{connector}/transfers/{id}",
            "trueconnector://{connector}/audit/types",
        ]

        listed = (await client.read_resource("trueconnector://connectors")).contents[0].text
        assert "SECRET" not in listed and "admin@mail.com" not in listed
        assert json.loads(listed) == [
            {
                "name": "a",
                "base_url": "http://tc-a",
                "protocol_url": "http://connector-a:8080/connector-a",
                "auth": "token",
            },
            {"name": "b", "base_url": "http://tc-b", "protocol_url": None, "auth": "password"},
            {"name": "c", "base_url": "http://tc-c", "protocol_url": None, "auth": "none"},
        ]

        catalog = await client.read_resource("trueconnector://c/catalog")
        assert json.loads(catalog.contents[0].text) == FIXTURES["envelope_data"]["body"]["data"]

        dataset = await client.read_resource("trueconnector://c/datasets/urn:uuid:d")
        assert json.loads(dataset.contents[0].text) == FIXTURES["dataset"]["body"]["data"]

        negotiation = await client.read_resource("trueconnector://c/negotiations/urn:uuid:n")
        assert json.loads(negotiation.contents[0].text) == FIXTURES["envelope_data"]["body"]["data"]

        transfer = await client.read_resource("trueconnector://c/transfers/urn:uuid:t")
        assert json.loads(transfer.contents[0].text) == FIXTURES["dataset"]["body"]["data"]

        types = await client.read_resource("trueconnector://c/audit/types")
        assert json.loads(types.contents[0].text) == FIXTURES["audit_types"]["body"]["data"]

        # A connector error reaches the client with its message, not as an anonymous crash.
        with pytest.raises(mcp.MCPError, match="Contract negotiation with id does-not-exist not found"):
            await client.read_resource("trueconnector://c/datasets/missing")
        with pytest.raises(mcp.MCPError, match="Unknown connector 'x'"):
            await client.read_resource("trueconnector://x/catalog")


async def test_publish_dataset_prompt():
    async with mcp.Client(build_server(SETTINGS)) as client:
        prompt = {p.name: p for p in (await client.list_prompts()).prompts}["publish_dataset"]
        args = {"title": "Sensors", "description": "hourly readings", "external_url": "https://data.example/s"}
        result = await client.get_prompt("publish_dataset", {**args, "connector": "b"})

    assert prompt.name == "publish_dataset"
    assert {a.name: a.required for a in prompt.arguments} == {
        "title": True,
        "description": True,
        "external_url": True,
        "connector": False,
    }
    text = result.messages[0].content.text
    # The prompt collects the policy from the user, then hands the whole chain to the publish_dataset tool.
    assert text.index("Ask me which usage policy") < text.index("Call publish_dataset(")
    assert 'external_url="https://data.example/s"' in text and 'connector="b"' in text
    assert "create_data_service" not in text


async def test_handle_incoming_negotiations_prompt_leaves_decisions_to_the_user():
    async with mcp.Client(build_server(SETTINGS)) as client:
        prompt = {p.name: p for p in (await client.list_prompts()).prompts}["handle_incoming_negotiations"]
        result = await client.get_prompt("handle_incoming_negotiations", {"connector": "b"})

    assert {a.name: a.required for a in prompt.arguments} == {"connector": True}
    text = result.messages[0].content.text
    assert "For each S in REQUESTED, ACCEPTED, VERIFIED: call list_negotiations(" in text
    assert 'list_negotiations(role="provider", state=S,' in text and 'connector="b"' in text
    assert "Never agree, counteroffer or terminate without my decision" in text
    assert text.index("For each VERIFIED one") > text.index("Never agree")
    assert 'list_transfers(role="provider", state="REQUESTED", connector="b")' in text
    assert "Never start or terminate a transfer without my decision" in text
    assert 'state="STARTED", format="HttpData-PUSH"' in text


async def consume(args):
    async with mcp.Client(build_server(SETTINGS)) as client:
        prompt = {p.name: p for p in (await client.list_prompts()).prompts}["consume_dataset"]
        return prompt, (await client.get_prompt("consume_dataset", args)).messages[0].content.text


async def test_consume_dataset_prompt_pull():
    prompt, text = await consume({"provider": "b", "dataset_hint": "sensors", "connector": "a"})

    assert {a.name: a.required for a in prompt.arguments} == {
        "provider": True,
        "dataset_hint": False,
        "format": False,
        "connector": False,
    }
    assert "HttpData-PULL" in text and 'matching "sensors"' in text
    assert "never choose for me" in text
    # Mode-agnostic: automatic mode is recognized by the states reached, never assumed.
    assert 'states=["OFFERED", "AGREED", "FINALIZED"], connector="a"' in text
    assert 'states=["STARTED", "DOWNLOADED"], connector="a"' in text
    assert 'move_transfer_data(transfer_process_id=<T>, connector="a")' in text
    assert 'connector="b"' not in text  # PULL never acts on the provider's side
    steps = ["request_contract(", "request_transfer(", "move_transfer_data(", "get_transfer_data_url("]
    assert [text.index(s) for s in steps] == sorted(text.index(s) for s in steps)


async def test_consume_dataset_prompt_push_moves_on_the_provider_only_with_consent():
    _, text = await consume({"provider": "b", "format": "HttpData-PUSH", "connector": "a"})

    assert "ask me before acting on its side" in text
    assert 'role="provider", connector="b"' in text
    assert 'move_transfer_data(transfer_process_id=<P>, connector="b")' in text
    assert "move_transfer_data(transfer_process_id=<T>" not in text
    assert text.index("ask me before acting") < text.index("move_transfer_data(")


async def test_consume_dataset_prompt_rejects_an_unknown_format():
    async with mcp.Client(build_server(SETTINGS)) as client:
        with pytest.raises(mcp.MCPError):
            await client.get_prompt("consume_dataset", {"provider": "b", "format": "FTP"})


async def diagnose(kind):
    async with mcp.Client(build_server(SETTINGS)) as client:
        prompt = {p.name: p for p in (await client.list_prompts()).prompts}["diagnose_process"]
        text = (await client.get_prompt("diagnose_process", {"kind": kind, "id": "x1", "connector": "b"})).messages
        annotations = {t.name: t.annotations for t in (await client.list_tools()).tools}
    return prompt, text[0].content.text, annotations


@pytest.mark.parametrize("kind", ["negotiation", "transfer"])
async def test_diagnose_process_allows_only_read_tools(kind):
    prompt, text, annotations = await diagnose(kind)

    assert {a.name: a.required for a in prompt.arguments} == {"kind": True, "id": True, "connector": True}
    allowed = re.search(r"read-only: call only ([a-z_, ]+)\.", text).group(1).split(", ")
    assert f"get_{kind}" in allowed and "list_audit_events" in allowed
    assert all(annotations[name].read_only_hint for name in allowed), allowed
    assert f'get_{kind}(id="x1", connector="b")' in text
    assert "details.consumerPid" in text
    assert (negotiation.STATE_MACHINE if kind == "negotiation" else transfer.STATE_MACHINE) in text
    for code in FAILURE_EVENTS[kind]:
        assert f'"{code}"' in text


async def test_diagnose_process_names_the_known_silent_transfer_failures():
    _, text, _ = await diagnose("transfer")

    assert 'list_audit_events(event_type=["TRANSFER_FAILED", ' in text
    assert "TRANSFER_FAILED event of the moving side" in text and "details.errorMessage" in text
    assert "artifactType EXTERNAL" in text and 'publish_dataset(store="copy")' in text
    assert "s3.externalPresignedEndpoint" in text
    assert "COMPLETED with downloaded=false" in text


async def test_diagnose_process_names_the_provider_started_negotiation_limit():
    _, text, _ = await diagnose("negotiation")

    assert "without callbackAddress was started by the provider" in text
    assert "OFFERED on the consumer" in text and "AGREED on the consumer" in text


def test_diagnose_process_names_only_event_codes_the_connector_has():
    known = {t["code"] for t in FIXTURES["audit_types"]["body"]["data"]}  # recorded from the live stack

    named = {code for codes in FAILURE_EVENTS.values() for code in codes}

    assert named <= known, named - known


async def test_diagnose_process_rejects_an_unknown_kind():
    async with mcp.Client(build_server(SETTINGS)) as client:
        with pytest.raises(mcp.MCPError):
            await client.get_prompt("diagnose_process", {"kind": "dataset", "id": "x", "connector": "b"})


@pytest.mark.parametrize(
    ("name", "args"),
    [
        ("publish_dataset", {"title": "t", "description": "d", "external_url": "https://x"}),
        ("handle_incoming_negotiations", {"connector": "b"}),
        ("consume_dataset", {"provider": "b"}),
        ("consume_dataset", {"provider": "b", "format": "HttpData-PUSH"}),
        ("diagnose_process", {"kind": "negotiation", "id": "n1", "connector": "b"}),
        ("diagnose_process", {"kind": "transfer", "id": "t1", "connector": "b"}),
    ],
)
async def test_prompts_only_name_tools_that_exist(name, args):
    async with mcp.Client(build_server(SETTINGS)) as client:
        tools = {t.name for t in (await client.list_tools()).tools}
        text = (await client.get_prompt(name, args)).messages[0].content.text

    called = set(re.findall(r"\b([a-z_]+)\(", text))
    assert called and called <= tools, called - tools
