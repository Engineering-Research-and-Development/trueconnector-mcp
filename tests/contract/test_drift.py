"""The TrueConnector endpoints the server calls exist in the management API snapshot, and the snapshot matches the
running API. Refresh the snapshot with `curl <connector>/v3/api-docs > management-api.json` (only /api/ paths count)."""

import json
import re
from pathlib import Path

import httpx2
import pytest

from tc_mcp.config import Settings
from tc_mcp.tools import negotiation, transfer

ROOT = Path(__file__).parents[2]
SNAPSHOT = json.loads(Path(__file__).with_name("management-api.json").read_text(encoding="utf-8"))
METHODS = {"get", "post", "put", "delete", "patch"}
# An /api/v1 path literal, with its method when it is the path of a request(...) call.
LITERAL = re.compile(r'(?:request\(\s*"(GET|POST|PUT|DELETE)",\s*)?f?"(/api/v1[^"]*)"')
# Paths the server builds from a variable, invisible to the regex.
BUILT = {
    *(("PUT", f"/api/v1/negotiations/{{}}/{action}") for action in (*negotiation.TRANSITIONS, "offer", "request")),
    *(("PUT", f"/api/v1/transfers/{{}}/{action}") for action in transfer.TRANSITIONS),
    ("GET", "/api/v1/transfers/{}/download"),
}


def called(source: str) -> set[tuple[str | None, str]]:
    """(method or None when unknown, path with every {expression} as {}) for each /api/v1 literal."""
    return {(method or None, re.sub(r"\{[^}]*\}", "{}", path)) for method, path in LITERAL.findall(source)}


def endpoints(openapi: dict) -> set[tuple[str, str]]:
    return {
        (method.upper(), path)
        for path, operations in openapi["paths"].items()
        if path.startswith("/api/")
        for method in operations
        if method in METHODS
    }


def missing(calls: set[tuple[str | None, str]]) -> set[tuple[str | None, str]]:
    """The calls no snapshot endpoint answers. A {} stands for any segment; literal segments must be equal."""

    def answers(method: str, path: str, call: tuple[str | None, str]) -> bool:
        want, segments = call[0], call[1].split("/")
        theirs = path.split("/")
        same = len(segments) == len(theirs) and all(s in ("{}", t) for s, t in zip(segments, theirs, strict=True))
        return same and want in (None, method)

    known = endpoints(SNAPSHOT)
    return {call for call in calls if not any(answers(method, path, call) for method, path in known)}


def test_every_called_endpoint_exists_in_the_snapshot():
    source = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "src").rglob("*.py"))
    calls = called(source) | BUILT

    assert missing(calls) == set()
    assert len(calls) > 40  # the extraction still sees the server's calls


def test_regex_extraction_detects_a_renamed_endpoint():
    source = """
        await client.request("GET", f"/api/v1/negotiation/{seg(id)}")
        await client.request("DELETE", "/api/v1/catalogs")
        await client.request("GET", "/api/v1/catalogs/all")
        register_readers(mcp, "dataset", "datasets", "/api/v1/datasets")
    """
    renamed = ("GET", "/api/v1/negotiation/{}")
    wrong_method = ("DELETE", "/api/v1/catalogs")
    literal_for_an_id = ("GET", "/api/v1/catalogs/all")  # GET /api/v1/catalogs/{id} must not answer it

    calls = called(source)

    assert calls == {renamed, wrong_method, literal_for_an_id, (None, "/api/v1/datasets")}
    assert missing(calls) == {renamed, wrong_method, literal_for_an_id}


@pytest.mark.integration
def test_the_running_api_matches_the_snapshot():
    snapshot = endpoints(SNAPSHOT)
    for connector in Settings(_env_file=ROOT / ".env.example").connectors:
        try:
            response = httpx2.get(f"{connector.base_url}/v3/api-docs", timeout=10)
            response.raise_for_status()
        except httpx2.HTTPError:
            pytest.skip(f"TrueConnector stack is down ({connector.name} at {connector.base_url})")
        running = endpoints(response.json())

        assert running == snapshot, (
            f"{connector.name} added: {sorted(running - snapshot)}, removed: {sorted(snapshot - running)}"
        )
