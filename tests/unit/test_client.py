import json

import anyio
import httpx2
import pytest
from conftest import FIXTURES, FakeConnector, live, login_response, make_client
from mcp.server.mcpserver.exceptions import ToolError

from tc_mcp.client import AlreadyInState, Registry, Rejected, TCClient, _error, _unwrap
from tc_mcp.config import Connector, Settings

pytestmark = pytest.mark.anyio


async def test_unwraps_envelope_data():
    client = make_client(FakeConnector({("GET", "/api/v1/catalogs"): "envelope_data"}))

    catalog = await client.request("GET", "/api/v1/catalogs")

    assert catalog == FIXTURES["envelope_data"]["body"]["data"]


async def test_unwraps_paged_list_without_hateoas_links():
    client = make_client(FakeConnector({("GET", "/api/v1/negotiations"): "paged"}))

    result = await client.request("GET", "/api/v1/negotiations")

    recorded = FIXTURES["paged"]["body"]["response"]["data"]
    assert "links" in recorded["content"][0]
    assert set(result) == {"items", "page"}
    assert result["page"] == recorded["page"]
    assert len(result["items"]) == len(recorded["content"]) > 0
    assert all("links" not in item for item in result["items"])
    assert result["items"][0]["@id"] == recorded["content"][0]["@id"]


async def test_envelope_without_data_returns_message():
    client = make_client(FakeConnector({("DELETE", "/api/v1/dataservices/x"): "envelope_no_data"}))

    assert await client.request("DELETE", "/api/v1/dataservices/x") == {"message": "Data service deleted successfully"}


def test_empty_data_is_data_not_a_message():
    # No live connector had an empty list to record, so take the real envelope and empty its data.
    assert _unwrap({**FIXTURES["envelope_data"]["body"], "data": []}) == []


@pytest.mark.parametrize("name", ["login", "dspace_version", "view"])
async def test_bare_bodies_pass_through(name):
    client = make_client(FakeConnector({("GET", "/x"): name}))

    assert await client.request("GET", "/x") == FIXTURES[name]["body"]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("error_400", "rejected: Contract negotiation with id does-not-exist not found"),
        ("error_401", "authentication failed"),
        ("error_401_refresh_reuse", "authentication failed"),
        ("error_403", "forbidden"),
        ("error_500", "internal error (Cannot invoke"),
    ],
)
async def test_error_shapes_map_to_tool_errors(name, expected):
    client = make_client(FakeConnector({("GET", "/x"): name}))

    with pytest.raises(ToolError) as err:
        await client.request("GET", "/x")

    assert str(err.value).startswith("a GET /x: " if name != "error_400" else "a GET /x rejected: ")
    assert expected in str(err.value)
    assert isinstance(err.value, Rejected) == (name == "error_400")
    assert not isinstance(err.value, AlreadyInState)


def test_unexpected_status_is_reported_with_its_code():
    assert "unexpected HTTP 404" in str(_error(404, {"message": "nope"}, "a GET /x"))


async def test_same_state_transition_is_already_in_state():
    client = make_client(FakeConnector({("PUT", "/x"): "error_400_already"}))

    with pytest.raises(AlreadyInState) as err:
        await client.request("PUT", "/x")

    assert err.value.state == "FINALIZED"
    assert err.value.reason == FIXTURES["error_400_already"]["body"]["message"]


def test_illegal_transition_is_a_plain_rejection():
    message = "State transition aborted, FINALIZED state can not transition to ACCEPTED"  # live, connector A

    err = _error(400, {"success": False, "message": message}, "a PUT /x")

    assert isinstance(err, Rejected)
    assert not isinstance(err, AlreadyInState)


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (httpx2.ConnectError("refused"), r"^b POST /x: connector unreachable at http://tc-b"),
        (httpx2.ReadTimeout("slow"), r"^b POST /x: timed out; it may still have been applied"),
    ],
)
async def test_unreachable_connector(exc, expected):
    def down(request):
        raise exc

    client = TCClient(Connector(name="b", base_url="http://tc-b"), timeout_s=5, transport=httpx2.MockTransport(down))

    with pytest.raises(ToolError, match=expected):
        await client.request("POST", "/x")


LOGIN, REFRESH, LOGOUT, CATALOGS = (
    ("POST", "/api/v1/auth/login"),
    ("POST", "/api/v1/auth/refresh"),
    ("POST", "/api/v1/auth/logout"),
    ("GET", "/api/v1/catalogs"),
)
CREDENTIALS = {"email": "admin@mail.com", "password": "pw"}


def bearer(request):
    return request.headers.get("Authorization")


def sent(request):
    return json.loads(request.content)


async def test_logs_in_lazily_once_and_sends_bearer():
    fake = FakeConnector({LOGIN: lambda r: login_response("A1", "R1"), CATALOGS: "envelope_data"})
    client = make_client(fake, **CREDENTIALS)
    assert fake.requests == []  # nothing happens until the first real call

    await client.request(*CATALOGS)
    await client.request(*CATALOGS)

    [login] = fake.calls(*LOGIN)
    assert sent(login) == CREDENTIALS
    assert bearer(login) is None
    assert [bearer(r) for r in fake.calls(*CATALOGS)] == ["Bearer A1", "Bearer A1"]


async def test_expired_token_is_refreshed_and_rotated_pair_stored():
    # expires_in=10 is inside the 30 s skew, so the first token already counts as expired.
    fake = FakeConnector(
        {
            LOGIN: lambda r: login_response("A1", "R1", expires_in=10),
            REFRESH: lambda r: login_response("A2", "R2"),
            LOGOUT: lambda r: httpx2.Response(200),
            CATALOGS: "envelope_data",
        }
    )
    client = make_client(fake, **CREDENTIALS)

    for _ in range(3):
        await client.request(*CATALOGS)
    await client.aclose()

    assert len(fake.calls(*LOGIN)) == 1
    [refresh] = fake.calls(*REFRESH)
    assert sent(refresh) == {"refresh_token": "R1"}
    assert [bearer(r) for r in fake.calls(*CATALOGS)] == ["Bearer A1", "Bearer A2", "Bearer A2"]
    [logout] = fake.calls(*LOGOUT)
    assert sent(logout) == {"refresh_token": "R2"}  # the rotated refresh token, not the revoked R1


async def test_rejected_refresh_falls_back_to_login():
    logins = iter([login_response("A1", "R1", expires_in=10), login_response("A3", "R3")])
    fake = FakeConnector({LOGIN: lambda r: next(logins), REFRESH: "error_401_refresh_reuse", CATALOGS: "envelope_data"})
    client = make_client(fake, **CREDENTIALS)

    await client.request(*CATALOGS)
    await client.request(*CATALOGS)

    assert len(fake.calls(*REFRESH)) == 1
    assert len(fake.calls(*LOGIN)) == 2
    assert bearer(fake.calls(*CATALOGS)[-1]) == "Bearer A3"


async def test_login_failure_is_reported():
    # A wrong password returns the same Spring 401 body as a reused refresh token (checked live).
    fake = FakeConnector({LOGIN: "error_401_refresh_reuse", CATALOGS: "envelope_data"})
    client = make_client(fake, **CREDENTIALS)

    with pytest.raises(ToolError, match=r"^a POST /api/v1/auth/login: authentication failed"):
        await client.request(*CATALOGS)
    assert fake.calls(*CATALOGS) == []


@pytest.mark.parametrize(("connector", "expected"), [({"token": "STATIC"}, "Bearer STATIC"), ({}, None)])
async def test_static_token_and_no_auth_modes_never_log_in(connector, expected):
    fake = FakeConnector({CATALOGS: "envelope_data"})  # no login route: a login attempt would fail the test
    client = make_client(fake, **connector)

    await client.request(*CATALOGS)
    await client.aclose()

    assert [bearer(r) for r in fake.requests] == [expected]


async def test_401_triggers_one_renewal_and_retry():
    catalogs = iter(["error_401", "envelope_data"])
    fake = FakeConnector(
        {
            LOGIN: lambda r: login_response("A1", "R1"),
            REFRESH: lambda r: login_response("A2", "R2"),
            CATALOGS: lambda r: live(next(catalogs)),
        }
    )
    client = make_client(fake, **CREDENTIALS)

    assert await client.request(*CATALOGS) == FIXTURES["envelope_data"]["body"]["data"]

    assert len(fake.calls(*REFRESH)) == 1
    assert [bearer(r) for r in fake.calls(*CATALOGS)] == ["Bearer A1", "Bearer A2"]


async def test_persistent_401_fails_after_a_single_retry():
    fake = FakeConnector(
        {
            LOGIN: lambda r: login_response("A1", "R1"),
            REFRESH: lambda r: login_response("A2", "R2"),
            CATALOGS: "error_401",
        }
    )
    client = make_client(fake, **CREDENTIALS)

    with pytest.raises(ToolError, match=r"^a GET /api/v1/catalogs: authentication failed"):
        await client.request(*CATALOGS)

    assert len(fake.calls(*CATALOGS)) == 2
    assert len(fake.calls(*REFRESH)) == 1


async def test_concurrent_requests_share_one_refresh():
    # FakeConnector yields on every request; a handler that never awaits runs the tasks one by one and hides the race.
    fake = FakeConnector(
        {
            LOGIN: lambda r: login_response("A1", "R1", expires_in=10),
            REFRESH: lambda r: login_response("A2", "R2"),
            CATALOGS: "envelope_data",
        }
    )
    client = make_client(fake, **CREDENTIALS)
    await client.request(*CATALOGS)  # logs in; the token is already inside the skew

    async with anyio.create_task_group() as tg:
        for _ in range(5):
            tg.start_soon(client.request, *CATALOGS)

    assert len(fake.calls(*REFRESH)) == 1
    assert [bearer(r) for r in fake.calls(*CATALOGS)[1:]] == ["Bearer A2"] * 5


async def test_registry_resolves_peers_and_connectors():
    settings = Settings(
        _env_file=None,
        connectors=[
            {"name": "a", "base_url": "http://tc-a", "protocol_url": "http://connector-a:8080/connector-a"},
            {"name": "b", "base_url": "http://tc-b"},
        ],
    )
    registry = Registry(settings)

    assert registry.get().connector.name == "a"
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


async def test_logout_failure_does_not_break_shutdown():
    fake = FakeConnector({LOGIN: lambda r: login_response("A1", "R1"), LOGOUT: "error_500", CATALOGS: "envelope_data"})
    client = make_client(fake, **CREDENTIALS)
    await client.request(*CATALOGS)

    await client.aclose()

    assert len(fake.calls(*LOGOUT)) == 1
