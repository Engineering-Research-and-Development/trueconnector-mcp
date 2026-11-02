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
