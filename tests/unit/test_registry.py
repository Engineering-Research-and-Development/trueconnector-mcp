import pytest
from mcp.server.mcpserver.exceptions import ToolError

from tc_mcp.config import Settings
from tc_mcp.registry import Registry

pytestmark = pytest.mark.anyio


async def test_registry_resolves_peers_and_connectors():
    settings = Settings(
        _env_file=None,
        connectors=[
            {"name": "a", "base_url": "http://tc-a", "protocol_url": "http://connector-a:8080/connector-a"},
            {"name": "b", "base_url": "http://tc-b"},
        ],
    )
    registry = Registry(settings)

    with pytest.raises(ToolError, match=r"Several connectors configured \(a, b\): pass connector"):
        registry.get()
    assert registry.get("b").connector.name == "b"
    with pytest.raises(ToolError, match=r"Unknown connector 'c'. Configured: a, b"):
        registry.get("c")

    assert registry.forward_to("a") == "http://connector-a:8080/connector-a"
    assert registry.forward_to("https://peer.example/dsp") == "https://peer.example/dsp"
    with pytest.raises(ToolError, match="'b' has no protocol_url"):
        registry.forward_to("b")
    with pytest.raises(ToolError, match="Unknown peer 'connector-b'"):
        registry.forward_to("connector-b")

    await registry.aclose()
    assert registry.get("a")._http.is_closed and registry.get("b")._http.is_closed

    only = Registry(Settings(_env_file=None, connectors=[{"name": "a", "base_url": "http://tc-a"}]))
    assert only.get().connector.name == "a"
