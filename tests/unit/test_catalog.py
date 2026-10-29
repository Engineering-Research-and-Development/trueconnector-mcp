import json

import httpx2
import mcp
import pytest
from conftest import FIXTURES, FakeConnector

from tc_mcp.config import Settings
from tc_mcp.server import build_server
from tc_mcp.tools.catalog import seg

pytestmark = pytest.mark.anyio

B_PROTOCOL = "http://connector-b:8080/connector-b"
SETTINGS = Settings(
    _env_file=None,
    connectors=[
        {"name": "a", "base_url": "http://tc-a"},
        {"name": "b", "base_url": "http://tc-b", "protocol_url": B_PROTOCOL},
    ],
)


async def call(fake: FakeConnector, tool: str, **arguments):
    async with mcp.Client(build_server(SETTINGS, httpx2.MockTransport(fake))) as client:
        return await client.call_tool(tool, arguments)


async def test_browse_remote_catalog_resolves_peer_and_summarizes():
    fake = FakeConnector({("POST", "/api/v1/proxy/catalogs"): "proxy_catalog"})

    result = await call(fake, "browse_remote_catalog", peer="b", connector="a")

    assert not result.is_error, result.content
    [request] = fake.requests
    assert request.url.host == "tc-a"  # asked through our own connector...
    assert json.loads(request.content) == {"Forward-To": B_PROTOCOL}  # ...to fetch b's catalog
    assert json.loads(result.content[0].text) == {
        "participantId": "urn:example:DataProviderA",
        "datasets": [
            {
                "id": "urn:uuid:fdc45798-a222-4955-8baf-ab7fd66ac4d5",
                "title": "TRUEConnector team information dataset",
                "description": "Dataset offering TRUEConnector instance B team information",
                "formats": ["HttpData-PULL", "HttpData-PUSH"],
                "offers": [
                    {
                        "id": "urn:uuid:fdc45798-a123-4955-8baf-ab7fd66ac4d5",
                        "permission": [
                            {
                                "action": "USE",
                                "constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": "5"}],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    full = await call(fake, "browse_remote_catalog", peer="b", connector="a", full=True)
    assert json.loads(full.content[0].text) == FIXTURES["proxy_catalog"]["body"]["data"]


async def test_browse_rejects_unknown_peer_without_calling_the_connector():
    fake = FakeConnector({})

    result = await call(fake, "browse_remote_catalog", peer="connector-c", connector="a")

    assert result.is_error
    assert "Unknown peer 'connector-c'" in result.content[0].text
    assert fake.requests == []


async def test_remote_formats_quote_the_dataset_id():
    fake = FakeConnector({("POST", "/api/v1/proxy/datasets/urn:uuid:x/y/formats"): "proxy_formats"})

    result = await call(fake, "get_remote_dataset_formats", peer="b", dataset_id="urn:uuid:x/y", connector="a")

    assert not result.is_error, result.content
    assert len(result.content) == 1
    assert json.loads(result.content[0].text) == {"formats": ["HttpData-PULL", "HttpData-PUSH"]}
    [request] = fake.requests
    assert request.url.raw_path == b"/api/v1/proxy/datasets/urn:uuid:x%2Fy/formats"  # one segment on the wire
    assert json.loads(request.content) == {"Forward-To": B_PROTOCOL}


async def test_remote_catalog_tool_warns_that_content_is_untrusted():
    async with mcp.Client(build_server(SETTINGS)) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}
    assert "untrusted" in tools["browse_remote_catalog"].description
    assert all(
        tools[n].annotations.read_only_hint
        for n in ("browse_remote_catalog", "get_remote_dataset_formats", "get_own_catalog")
    )


def test_seg_keeps_urn_colons_and_quotes_path_characters():
    assert seg("urn:uuid:1") == "urn:uuid:1"
    assert seg("a/b?c#d e") == "a%2Fb%3Fc%23d%20e"
