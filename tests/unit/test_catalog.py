import json

import mcp
import pytest
from helpers import B_PROTOCOL, FIXTURES, TWO_CONNECTORS, FakeConnector, call

from tc_mcp.server import build_server

pytestmark = pytest.mark.anyio


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
    async with mcp.Client(build_server(TWO_CONNECTORS)) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}
    assert "untrusted" in tools["browse_remote_catalog"].description


async def test_update_catalog_sends_only_the_given_fields():
    # Live: catalog PUT merges server-side (unlike datasets), so no GET-merge is needed.
    fake = FakeConnector({("PUT", "/api/v1/catalogs/urn:uuid:c"): "envelope_data"})

    result = await call(fake, "update_catalog", id="urn:uuid:c", catalog={"title": "new"}, connector="a")

    assert not result.is_error, result.content
    [request] = fake.requests
    assert json.loads(request.content) == {"title": "new"}
