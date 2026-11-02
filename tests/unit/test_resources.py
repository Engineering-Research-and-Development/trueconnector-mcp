import json

import httpx2
import mcp
import pytest
from conftest import FIXTURES, FakeConnector

from tc_mcp.config import Settings
from tc_mcp.server import build_server

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
        }
    )
    async with mcp.Client(build_server(SETTINGS, httpx2.MockTransport(fake))) as client:
        assert [t.uri_template for t in (await client.list_resource_templates()).resource_templates] == [
            "trueconnector://{connector}/catalog",
            "trueconnector://{connector}/datasets/{id}",
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

        # A connector error reaches the client with its message, not as an anonymous crash.
        with pytest.raises(mcp.MCPError, match="Contract negotiation with id does-not-exist not found"):
            await client.read_resource("trueconnector://c/datasets/missing")
        with pytest.raises(mcp.MCPError, match="Unknown connector 'x'"):
            await client.read_resource("trueconnector://x/catalog")


async def test_publish_dataset_prompt():
    async with mcp.Client(build_server(SETTINGS)) as client:
        [prompt] = (await client.list_prompts()).prompts
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
