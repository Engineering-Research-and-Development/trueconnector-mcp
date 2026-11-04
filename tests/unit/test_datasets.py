import json

import httpx2
import mcp
import pytest
from helpers import COUNT_5, DATASET, DISTRIBUTION, FIXTURES, SERVICE, TWO_CONNECTORS, FakeConnector, call, form_parts

from tc_mcp.server import build_server

pytestmark = pytest.mark.anyio

DATA = b'{"t": 1}'
SOURCE = ("GET", "/sensors.json")


def source(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=DATA, headers={"content-type": "text/csv; charset=utf-8"})


UNTIL_2030 = {
    "action": "USE",
    "constraint": [{"leftOperand": "DATE_TIME", "operator": "LT", "rightOperand": "2030-01-01T00:00:00Z"}],
}
DATASET_ARGS = {
    "title": "sensors",
    "description": "hourly readings",
    "distribution_ids": [DISTRIBUTION["@id"]],
    "offers": [COUNT_5, UNTIL_2030],
    "external_url": "https://data.example/sensors.json",
    "connector": "a",
}
PUBLISH_ARGS = {
    "title": "sensors",
    "description": "hourly readings",
    "offers": [COUNT_5],
    "external_url": "https://data.example/sensors.json",
    "connector": "a",
}
SERVICE_PATH = f"/api/v1/dataservices/{SERVICE['@id']}"
DISTRIBUTION_PATH = f"/api/v1/distributions/{DISTRIBUTION['@id']}"


async def test_create_dataset_sends_multipart_with_resolved_distributions():
    fake = FakeConnector(
        {
            SOURCE: source,
            ("GET", f"/api/v1/distributions/{DISTRIBUTION['@id']}"): "distribution",
            ("POST", "/api/v1/datasets"): "dataset",
        }
    )

    extra = {"external_authorization": "Bearer s3cret", "keywords": ["iot"]}
    result = await call(fake, "create_dataset", **DATASET_ARGS, **extra)

    assert not result.is_error, result.content
    [post] = fake.calls("POST", "/api/v1/datasets")
    assert post.headers["content-type"].startswith("multipart/form-data; boundary=")
    parts = form_parts(post)
    assert json.loads(parts.pop("dataset")) == {
        "title": "sensors",
        "description": [{"value": "hourly readings", "language": "en"}],
        "keyword": ["iot"],
        "distribution": [DISTRIBUTION],  # resolved from its id
        "hasPolicy": [{"permission": [COUNT_5]}, {"permission": [UNTIL_2030]}],  # one offer per permission
    }
    assert parts == {"file": DATA.decode()}
    assert b'filename="sensors.json"' in post.content and b"Content-Type: text/csv\r\n" in post.content
    [download] = fake.calls(*SOURCE)
    assert (download.url.host, download.headers["authorization"]) == ("data.example", "Bearer s3cret")


async def test_create_dataset_link_keeps_only_the_url():
    fake = FakeConnector(
        {
            ("GET", f"/api/v1/distributions/{DISTRIBUTION['@id']}"): "distribution",
            ("POST", "/api/v1/datasets"): "dataset",
        }
    )

    result = await call(fake, "create_dataset", **DATASET_ARGS, store="link", external_authorization="Bearer s3cret")

    assert not result.is_error, result.content
    parts = form_parts(fake.calls("POST", "/api/v1/datasets")[0])
    del parts["dataset"]
    assert parts == {"url": "https://data.example/sensors.json", "authorization": "Bearer s3cret"}
    assert not fake.calls(*SOURCE)  # the connector fetches it, not this server


def too_big(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=b"x" * 1001)


def unreachable(request: httpx2.Request) -> httpx2.Response:
    raise httpx2.ConnectError("name resolution failed", request=request)


@pytest.mark.parametrize(
    ("answer", "reason"),
    [
        (lambda _: httpx2.Response(404), "HTTP 404"),
        (unreachable, "ConnectError"),
        (too_big, "larger than 0.001 MB (TC_MAX_COPY_MB)"),
    ],
    ids=["missing", "unreachable", "too big"],
)
async def test_publish_dataset_creates_nothing_when_the_source_cannot_be_copied(answer, reason):
    fake = FakeConnector({SOURCE: answer})
    settings = TWO_CONNECTORS.model_copy(update={"max_copy_mb": 0.001})

    async with mcp.Client(build_server(settings, httpx2.MockTransport(fake))) as client:
        result = await client.call_tool("publish_dataset", PUBLISH_ARGS)

    assert result.is_error
    assert f"could not copy https://data.example/sensors.json: {reason}" in result.content[0].text
    assert [r.url.path for r in fake.requests] == ["/sensors.json"]  # nothing created on the connector


async def test_a_redirected_source_is_followed():
    fake = FakeConnector(
        {
            SOURCE: lambda _: httpx2.Response(302, headers={"location": "https://cdn.example/real.json"}),
            ("GET", "/real.json"): source,
            ("GET", f"/api/v1/distributions/{DISTRIBUTION['@id']}"): "distribution",
            ("POST", "/api/v1/datasets"): "dataset",
        }
    )

    result = await call(fake, "create_dataset", **DATASET_ARGS)

    assert not result.is_error, result.content
    assert form_parts(fake.calls("POST", "/api/v1/datasets")[0])["file"] == DATA.decode()


async def test_a_source_of_exactly_the_limit_is_copied():
    fake = FakeConnector(
        {
            SOURCE: lambda _: httpx2.Response(200, content=b"x" * 1000),
            ("POST", "/api/v1/dataservices"): "data_service",
            ("POST", "/api/v1/distributions"): "distribution",
            ("POST", "/api/v1/datasets"): "dataset",
        }
    )
    settings = TWO_CONNECTORS.model_copy(update={"max_copy_mb": 0.001})

    async with mcp.Client(build_server(settings, httpx2.MockTransport(fake))) as client:
        result = await client.call_tool("publish_dataset", PUBLISH_ARGS)

    assert not result.is_error, result.content
    assert b"Content-Type: application/octet-stream" in fake.calls("POST", "/api/v1/datasets")[0].content


@pytest.mark.parametrize(
    "bad",
    [
        {"distribution_ids": []},
        {"offers": []},
        {"external_url": "ftp://data.example/x"},
        {"external_url": "file:///etc/passwd"},
        {"external_url": "https://"},
        {"offers": [{"constraint": [{"leftOperand": "PURPOSE", "operator": "EQ", "rightOperand": "x"}]}]},
    ],
)
async def test_create_dataset_validates_inputs_before_sending(bad):
    fake = FakeConnector({})

    result = await call(fake, "create_dataset", **{**DATASET_ARGS, **bad})

    assert result.is_error
    assert fake.requests == []


async def test_update_dataset_merges_only_given_fields():
    path = f"/api/v1/datasets/{DATASET['@id']}"
    fake = FakeConnector({("GET", path): "dataset", ("PUT", path): "dataset"})

    result = await call(fake, "update_dataset", id=DATASET["@id"], title="renamed", connector="a")

    assert not result.is_error, result.content
    assert [r.method for r in fake.requests] == ["GET", "PUT"]  # no distribution lookups
    parts = form_parts(fake.requests[1])
    assert json.loads(parts.pop("dataset")) == {**DATASET, "title": "renamed"}
    assert parts == {}  # no url part: the artifact is kept


async def test_update_dataset_replaces_offers_distributions_and_artifact_when_given():
    path = f"/api/v1/datasets/{DATASET['@id']}"
    fake = FakeConnector(
        {
            ("GET", "/v2"): source,
            ("GET", path): "dataset",
            ("GET", f"/api/v1/distributions/{DISTRIBUTION['@id']}"): "distribution",
            ("PUT", path): "dataset",
        }
    )

    await call(
        fake,
        "update_dataset",
        id=DATASET["@id"],
        distribution_ids=[DISTRIBUTION["@id"]],
        offers=[COUNT_5],
        external_url="http://data.example/v2",
        connector="a",
    )

    [put] = fake.calls("PUT", path)
    parts = form_parts(put)
    dataset = json.loads(parts.pop("dataset"))
    assert (dataset["distribution"], dataset["hasPolicy"], dataset["title"]) == (
        [DISTRIBUTION],
        [{"permission": [COUNT_5]}],
        DATASET["title"],
    )
    assert parts == {"file": DATA.decode()}
    assert b'filename="v2"' in put.content


async def test_publish_dataset_chains_service_distribution_and_dataset():
    fake = FakeConnector(
        {
            SOURCE: source,
            ("POST", "/api/v1/dataservices"): "data_service",
            ("POST", "/api/v1/distributions"): "distribution",
            ("POST", "/api/v1/datasets"): "dataset",
        }
    )

    result = await call(fake, "publish_dataset", **PUBLISH_ARGS)

    assert not result.is_error, result.content
    assert json.loads(result.content[0].text) == DATASET
    assert [r.url.path for r in fake.requests] == [
        "/sensors.json",  # the source is copied before anything is created
        "/api/v1/dataservices",
        "/api/v1/distributions",
        "/api/v1/datasets",
    ]
    service, distribution = (json.loads(r.content) for r in fake.requests[1:3])
    assert service == {"title": "sensors service"}
    assert distribution == {"title": "sensors (HttpData-PULL)", "format": "HttpData-PULL", "accessService": SERVICE}
    parts = form_parts(fake.requests[3])
    dataset = json.loads(parts.pop("dataset"))
    assert dataset["distribution"] == [DISTRIBUTION]  # the created one, as the connector returned it
    assert dataset["hasPolicy"] == [{"permission": [COUNT_5]}]
    assert parts == {"file": DATA.decode()}


async def test_publish_dataset_deletes_what_it_created_when_a_step_fails():
    fake = FakeConnector(
        {
            SOURCE: source,
            ("POST", "/api/v1/dataservices"): "data_service",
            ("POST", "/api/v1/distributions"): "distribution",
            ("POST", "/api/v1/datasets"): "error_500",
            ("DELETE", DISTRIBUTION_PATH): "envelope_no_data",
            ("DELETE", SERVICE_PATH): "envelope_no_data",
        }
    )

    result = await call(fake, "publish_dataset", **PUBLISH_ARGS)

    assert result.is_error
    assert "connector internal error" in result.content[0].text
    assert "deleted the 2 object(s) it had created" in result.content[0].text
    assert [(r.method, r.url.path) for r in fake.requests[4:]] == [
        ("DELETE", DISTRIBUTION_PATH),
        ("DELETE", SERVICE_PATH),
    ]


async def test_publish_dataset_rollback_survives_a_failing_delete():
    fake = FakeConnector(
        {
            SOURCE: source,
            ("POST", "/api/v1/dataservices"): "data_service",
            ("POST", "/api/v1/distributions"): "error_400",
            ("DELETE", SERVICE_PATH): "error_500",
        }
    )

    result = await call(fake, "publish_dataset", **PUBLISH_ARGS)

    assert result.is_error
    text = result.content[0].text
    assert "not found" in text and "deleted the 1 object(s)" in text  # the original error, not the cleanup's
    assert fake.calls("DELETE", SERVICE_PATH)


async def test_dataset_formats_and_artifacts_are_wrapped():
    fake = FakeConnector(
        {
            ("GET", "/api/v1/datasets/urn:uuid:d/formats"): "proxy_formats",
            ("GET", "/api/v1/artifacts"): "proxy_formats",
            ("GET", "/api/v1/artifacts/urn:uuid:art"): "proxy_formats",
        }
    )
    data = FIXTURES["proxy_formats"]["body"]["data"]

    formats = await call(fake, "get_dataset_formats", id="urn:uuid:d", connector="a")
    every = await call(fake, "get_artifacts", connector="a")
    one = await call(fake, "get_artifacts", artifact_id="urn:uuid:art", connector="a")

    assert json.loads(formats.content[0].text) == {"formats": data}
    assert json.loads(every.content[0].text) == {"artifacts": data}
    assert json.loads(one.content[0].text) == {"artifacts": data}
    assert [r.url.path for r in fake.requests] == [
        "/api/v1/datasets/urn:uuid:d/formats",
        "/api/v1/artifacts",
        "/api/v1/artifacts/urn:uuid:art",
    ]
