import json
from email.parser import BytesParser
from email.policy import default

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


async def test_tool_catalog_and_annotations():
    async with mcp.Client(build_server(SETTINGS)) as client:
        tools = {t.name: t.annotations for t in (await client.list_tools()).tools}

    reads = {"browse_remote_catalog", "get_remote_dataset_formats", "get_own_catalog", "get_catalog"}
    reads |= {
        f"{verb}_{kind}" for verb, kind in [("list", "datasets"), ("list", "distributions"), ("list", "data_services")]
    }
    reads |= {f"get_{kind}" for kind in ("dataset", "distribution", "data_service")}
    reads |= {"get_dataset_formats", "get_dataset_artifact", "get_artifacts"}
    kinds = ("catalog", "data_service", "distribution", "dataset")
    writes = {f"{verb}_{kind}" for verb in ("create", "update") for kind in kinds} | {"publish_dataset"}
    deletes = {f"delete_{kind}" for kind in ("catalog", "dataset", "distribution", "data_service")}
    assert set(tools) == reads | writes | deletes | {"get_dspace_version"}
    for name in reads:
        assert tools[name].read_only_hint, name
    for name in writes:
        assert (tools[name].read_only_hint, tools[name].destructive_hint, tools[name].idempotent_hint) == (
            False,
            False,
            False,
        )
    for name in deletes:
        assert (tools[name].read_only_hint, tools[name].destructive_hint) == (False, True), name


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


async def test_update_catalog_sends_only_the_given_fields():
    # Live: catalog PUT merges server-side (unlike datasets), so no GET-merge is needed.
    fake = FakeConnector({("PUT", "/api/v1/catalogs/urn:uuid:c"): "envelope_data"})

    result = await call(fake, "update_catalog", id="urn:uuid:c", catalog={"title": "new"}, connector="a")

    assert not result.is_error, result.content
    [request] = fake.requests
    assert json.loads(request.content) == {"title": "new"}


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


SERVICE = FIXTURES["data_service"]["body"]["data"]
DISTRIBUTION = FIXTURES["distribution"]["body"]["data"]
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


DATASET = FIXTURES["dataset"]["body"]["data"]
COUNT_5 = {"action": "USE", "constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": "5"}]}
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


def form_parts(request: httpx2.Request) -> dict[str, str]:
    """The multipart/form-data fields of a request, decoded with the stdlib email parser."""
    head = f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode()
    message = BytesParser(policy=default).parsebytes(head + request.content)
    return {
        p.get_param("name", header="content-disposition"): p.get_payload(decode=True).decode()
        for p in message.iter_parts()
    }


async def test_create_dataset_sends_multipart_with_resolved_distributions():
    fake = FakeConnector(
        {
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
    assert parts == {"url": "https://data.example/sensors.json", "authorization": "Bearer s3cret"}


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

    parts = form_parts(fake.calls("PUT", path)[0])
    dataset = json.loads(parts.pop("dataset"))
    assert (dataset["distribution"], dataset["hasPolicy"], dataset["title"]) == (
        [DISTRIBUTION],
        [{"permission": [COUNT_5]}],
        DATASET["title"],
    )
    assert parts == {"url": "http://data.example/v2"}


PUBLISH_ARGS = {
    "title": "sensors",
    "description": "hourly readings",
    "offers": [COUNT_5],
    "external_url": "https://data.example/sensors.json",
    "connector": "a",
}
SERVICE_PATH = f"/api/v1/dataservices/{SERVICE['@id']}"
DISTRIBUTION_PATH = f"/api/v1/distributions/{DISTRIBUTION['@id']}"


async def test_publish_dataset_chains_service_distribution_and_dataset():
    fake = FakeConnector(
        {
            ("POST", "/api/v1/dataservices"): "data_service",
            ("POST", "/api/v1/distributions"): "distribution",
            ("POST", "/api/v1/datasets"): "dataset",
        }
    )

    result = await call(fake, "publish_dataset", **PUBLISH_ARGS)

    assert not result.is_error, result.content
    assert json.loads(result.content[0].text) == DATASET
    assert [r.url.path for r in fake.requests] == ["/api/v1/dataservices", "/api/v1/distributions", "/api/v1/datasets"]
    service, distribution = (json.loads(r.content) for r in fake.requests[:2])
    assert service == {"title": "sensors service"}
    assert distribution == {"title": "sensors (HttpData-PULL)", "format": "HttpData-PULL", "accessService": SERVICE}
    parts = form_parts(fake.requests[2])
    dataset = json.loads(parts.pop("dataset"))
    assert dataset["distribution"] == [DISTRIBUTION]  # the created one, as the connector returned it
    assert dataset["hasPolicy"] == [{"permission": [COUNT_5]}]
    assert parts == {"url": "https://data.example/sensors.json"}


async def test_publish_dataset_deletes_what_it_created_when_a_step_fails():
    fake = FakeConnector(
        {
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
    assert [(r.method, r.url.path) for r in fake.requests[3:]] == [
        ("DELETE", DISTRIBUTION_PATH),
        ("DELETE", SERVICE_PATH),
    ]


async def test_publish_dataset_rollback_survives_a_failing_delete():
    fake = FakeConnector(
        {
            ("POST", "/api/v1/dataservices"): "data_service",
            ("POST", "/api/v1/distributions"): "error_400",
            ("DELETE", SERVICE_PATH): "error_500",
        }
    )

    result = await call(fake, "publish_dataset", **PUBLISH_ARGS)

    assert result.is_error
    text = result.content[0].text
    assert "rejected" in text and "deleted the 1 object(s)" in text  # the original error, not the cleanup's
    assert fake.calls("DELETE", SERVICE_PATH)
