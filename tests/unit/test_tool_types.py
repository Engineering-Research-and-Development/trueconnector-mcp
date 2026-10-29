import mcp
import pytest
from mcp.server.mcpserver import MCPServer

from tc_mcp.tools import Permission, lang, text

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
