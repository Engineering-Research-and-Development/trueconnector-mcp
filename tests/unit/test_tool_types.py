import json

import mcp
import pytest
from helpers import FIXTURES, FakeConnector, call
from mcp.server.mcpserver import MCPServer

from tc_mcp.tools import Permission, lang, seg, text

pytestmark = pytest.mark.anyio


async def test_invalid_left_operand_is_rejected_before_the_tool_runs():
    server = MCPServer("t")
    ran = []

    @server.tool()
    async def publish(permissions: list[Permission]) -> str:
        ran.append(permissions)  # stands in for the HTTP call
        return "sent"

    bad = [{"constraint": [{"leftOperand": "PURPOSE", "operator": "EQ", "rightOperand": "x"}]}]
    good = [{"constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": "5"}]}]
    async with mcp.Client(server) as client:
        rejected = await client.call_tool("publish", {"permissions": bad})
        assert rejected.is_error
        assert "leftOperand" in rejected.content[0].text

        # Unconstrained permissions negotiate fine but the download always fails, so they are refused up front.
        for unconstrained in ({"constraint": []}, {"action": "USE"}):
            refused = await client.call_tool("publish", {"permissions": [unconstrained]})
            assert refused.is_error
            assert "constraint" in refused.content[0].text
        assert ran == []

        accepted = await client.call_tool("publish", {"permissions": good})
        assert not accepted.is_error, accepted.content
        assert ran[0][0].model_dump() == {"action": "USE", "constraint": good[0]["constraint"]}


def test_multilingual_text_helpers():
    assert lang("hi") == [{"value": "hi", "language": "en"}]
    assert text(lang("hi")) == "hi"
    assert text("plain") == "plain"
    assert text(None) is None
    assert text([]) is None


def test_seg_keeps_urn_colons_and_quotes_path_characters():
    assert seg("urn:uuid:1") == "urn:uuid:1"
    assert seg("a/b?c#d e") == "a%2Fb%3Fc%23d%20e"


@pytest.mark.parametrize(
    ("kind", "plural", "path"),
    [
        ("dataset", "datasets", "/api/v1/datasets"),
        ("distribution", "distributions", "/api/v1/distributions"),
        ("data_service", "data_services", "/api/v1/dataservices"),
    ],
)
async def test_readers_list_get_and_delete_each_resource_kind(kind, plural, path):
    fake = FakeConnector(
        {
            ("GET", path): "proxy_formats",  # any recorded list envelope
            ("GET", f"{path}/urn:uuid:x/y"): "envelope_data",
            ("DELETE", f"{path}/urn:uuid:x/y"): "envelope_no_data",
        }
    )

    listed = await call(fake, f"list_{plural}", connector="a")
    got = await call(fake, f"get_{kind}", id="urn:uuid:x/y", connector="a")
    deleted = await call(fake, f"delete_{kind}", id="urn:uuid:x/y", connector="a")

    assert len(listed.content) == 1
    assert json.loads(listed.content[0].text) == {plural: FIXTURES["proxy_formats"]["body"]["data"]}
    assert json.loads(got.content[0].text) == FIXTURES["envelope_data"]["body"]["data"]
    assert json.loads(deleted.content[0].text) == {"message": FIXTURES["envelope_no_data"]["body"]["message"]}
    assert [r.url.raw_path for r in fake.requests[1:]] == [f"{path}/urn:uuid:x%2Fy".encode()] * 2
