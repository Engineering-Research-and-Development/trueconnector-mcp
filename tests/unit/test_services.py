import json

import httpx2
import pytest
from helpers import DISTRIBUTION, SERVICE, FakeConnector, call

pytestmark = pytest.mark.anyio

OTHER = {**SERVICE, "@id": "urn:uuid:other", "title": "other service"}


async def test_create_distribution_embeds_the_data_service():
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/dataservices/{SERVICE['@id']}"): "data_service",
            ("POST", "/api/v1/distributions"): "distribution",
        }
    )

    result = await call(
        fake,
        "create_distribution",
        title="pull",
        format="HttpData-PULL",
        access_service_id=SERVICE["@id"],
        description="via http",
        connector="a",
    )

    assert not result.is_error, result.content
    posted = json.loads(fake.calls("POST", "/api/v1/distributions")[0].content)
    assert posted == {
        "title": "pull",
        "description": [{"value": "via http", "language": "en"}],
        "format": "HttpData-PULL",
        "accessService": SERVICE,  # the full object: a bare {"@id"} is echoed back with every field null
    }


async def test_create_distribution_rejects_an_unknown_format_before_sending():
    fake = FakeConnector({})

    result = await call(
        fake, "create_distribution", title="t", format="HTTP:PULL", access_service_id="s", connector="a"
    )

    assert result.is_error
    assert fake.requests == []


async def test_update_distribution_merges_onto_the_current_one():
    path = f"/api/v1/distributions/{DISTRIBUTION['@id']}"
    fake = FakeConnector({("GET", path): "distribution", ("PUT", path): "distribution"})

    result = await call(fake, "update_distribution", id=DISTRIBUTION["@id"], title="renamed", connector="a")

    assert not result.is_error, result.content
    assert [r.method for r in fake.requests] == ["GET", "PUT"]  # no data service lookup when it isn't changed
    assert json.loads(fake.requests[1].content) == {**DISTRIBUTION, "title": "renamed"}


async def test_update_distribution_can_switch_format_and_service():
    assert DISTRIBUTION["accessService"]["@id"] != "urn:uuid:other"
    path = f"/api/v1/distributions/{DISTRIBUTION['@id']}"
    fake = FakeConnector(
        {
            ("GET", path): "distribution",
            ("GET", "/api/v1/dataservices/urn:uuid:other"): lambda _: httpx2.Response(
                200, json={"success": True, "data": OTHER}
            ),
            ("PUT", path): "distribution",
        }
    )

    await call(
        fake,
        "update_distribution",
        id=DISTRIBUTION["@id"],
        format="HttpData-PUSH",
        access_service_id="urn:uuid:other",
        connector="a",
    )

    put = json.loads(fake.calls("PUT", path)[0].content)
    assert (put["format"], put["accessService"], put["title"]) == ("HttpData-PUSH", OTHER, DISTRIBUTION["title"])


async def test_data_service_create_and_update_send_only_the_given_fields():
    fake = FakeConnector(
        {
            ("POST", "/api/v1/dataservices"): "data_service",
            ("PUT", "/api/v1/dataservices/urn:uuid:s"): "data_service",
        }
    )

    await call(fake, "create_data_service", title="svc", keywords=["k"], connector="a")
    await call(fake, "update_data_service", id="urn:uuid:s", description="new text", connector="a")

    created, updated = (json.loads(r.content) for r in fake.requests)
    assert created == {"title": "svc", "keyword": ["k"]}  # no endpointURL: the connector sets its own
    assert updated == {"description": [{"value": "new text", "language": "en"}]}  # PUT merges server-side
