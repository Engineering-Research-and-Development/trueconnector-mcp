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
