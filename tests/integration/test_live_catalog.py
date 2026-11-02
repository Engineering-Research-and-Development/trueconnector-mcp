import pytest

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_browse_b_from_a_matches_b_own_catalog(call):
    own = await call("get_own_catalog", connector="b")
    summary = await call("browse_remote_catalog", peer="b", connector="a")

    assert [d["id"] for d in summary["datasets"]] == [d["@id"] for d in own["dataset"]]
    dataset = summary["datasets"][0]
    assert dataset["offers"], "the seed dataset has an offer"
    formats = await call("get_remote_dataset_formats", peer="b", dataset_id=dataset["id"], connector="a")
    assert sorted(formats["formats"]) == dataset["formats"]


async def test_readers_on_seed_data(call):
    own = await call("get_own_catalog", connector="a")
    assert (await call("get_catalog", id=own["@id"], connector="a"))["@id"] == own["@id"]

    [dataset] = (await call("list_datasets", connector="a"))["datasets"]
    assert (await call("get_dataset", id=dataset["@id"], connector="a"))["title"] == dataset["title"]
    assert (await call("get_dataset_formats", id=dataset["@id"], connector="a"))["formats"]

    artifact = await call("get_dataset_artifact", id=dataset["@id"], connector="a")
    assert (await call("get_artifacts", artifact_id=artifact["id"], connector="a"))["artifacts"][0]["id"] == artifact[
        "id"
    ]

    for plural, singular in [("distributions", "distribution"), ("data_services", "data_service")]:
        first = (await call(f"list_{plural}", connector="a"))[plural][0]
        assert (await call(f"get_{singular}", id=first["@id"], connector="a"))["@id"] == first["@id"]


async def test_catalog_create_update_delete(call, mcp_client):
    created = await call(
        "create_catalog",
        catalog={"title": "mcp test", "participantId": "urn:mcp-test", "keyword": ["k"]},
        connector="b",
    )
    try:
        updated = await call("update_catalog", id=created["@id"], catalog={"title": "mcp test v2"}, connector="b")
        assert (updated["title"], updated["participantId"], updated["keyword"]) == (
            "mcp test v2",
            "urn:mcp-test",
            ["k"],
        )
    finally:
        await call("delete_catalog", id=created["@id"], connector="b")
    gone = await mcp_client.call_tool("get_catalog", {"id": created["@id"], "connector": "b"})
    assert gone.is_error and "not found" in gone.content[0].text


async def test_data_service_and_distribution_create_update_delete(call):
    service = await call("create_data_service", title="mcp svc", keywords=["k"], connector="b")
    try:
        assert service["endpointURL"] == "http://connector-b:8080/connector-b"  # set by the connector itself
        service = await call("update_data_service", id=service["@id"], title="mcp svc v2", connector="b")
        assert (service["title"], service["keyword"]) == ("mcp svc v2", ["k"])

        dist = await call(
            "create_distribution",
            title="mcp dist",
            format="HttpData-PULL",
            access_service_id=service["@id"],
            connector="b",
        )
        try:
            assert dist["accessService"]["title"] == "mcp svc v2"
            dist = await call("update_distribution", id=dist["@id"], format="HttpData-PUSH", connector="b")
            assert (dist["title"], dist["format"]) == ("mcp dist", "HttpData-PUSH")
            assert dist["accessService"]["@id"] == service["@id"]
        finally:
            await call("delete_distribution", id=dist["@id"], connector="b")
    finally:
        await call("delete_data_service", id=service["@id"], connector="b")


async def test_dataset_create_update_delete(call):
    service = await call("create_data_service", title="mcp svc", connector="b")
    dist = await call(
        "create_distribution", title="mcp dist", format="HttpData-PULL", access_service_id=service["@id"], connector="b"
    )
    count = {"action": "USE", "constraint": [{"leftOperand": "COUNT", "operator": "LTEQ", "rightOperand": "3"}]}
    until = {
        "action": "USE",
        "constraint": [{"leftOperand": "DATE_TIME", "operator": "LT", "rightOperand": "2030-01-01T00:00:00Z"}],
    }
    try:
        dataset = await call(
            "create_dataset",
            title="mcp dataset",
            description="created by the MCP tests",
            distribution_ids=[dist["@id"]],
            offers=[count, until],
            external_url="https://example.com/data.json",
            connector="b",
        )
        try:
            assert (dataset["artifact"]["artifactType"], dataset["artifact"]["value"]) == (
                "EXTERNAL",
                "https://example.com/data.json",
            )
            offer_ids = [o["@id"] for o in dataset["hasPolicy"]]
            assert len(offer_ids) == 2 and all(offer_ids)  # one offer per permission, ids set by the connector

            renamed = await call("update_dataset", id=dataset["@id"], title="mcp dataset v2", connector="b")
            assert renamed["title"] == "mcp dataset v2"
            assert [o["@id"] for o in renamed["hasPolicy"]] == offer_ids
            assert renamed["distribution"][0]["@id"] == dist["@id"]

            moved = await call(
                "update_dataset", id=dataset["@id"], external_url="https://example.com/v2", connector="b"
            )
            assert (moved["title"], moved["artifact"]["value"]) == ("mcp dataset v2", "https://example.com/v2")
        finally:
            await call("delete_dataset", id=dataset["@id"], connector="b")
    finally:
        await call("delete_distribution", id=dist["@id"], connector="b")
        await call("delete_data_service", id=service["@id"], connector="b")
