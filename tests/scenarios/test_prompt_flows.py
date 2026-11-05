"""The prompts' flows, step by step, against the Dockerized server over streamable HTTP. Each step checks that the
prompt really tells the agent to make that call, so a prompt that drifts from what works fails here."""

import httpx2
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.anyio]

SEEDED_DATA = [{"employee": {"name": "John Doe", "age": 30, "city": "New York"}}]  # the stack's seeded dataset


async def prompt(http_client, name, **args):
    """The prompt's text with all whitespace runs as one space, so rewrapping it changes nothing here."""
    return " ".join((await http_client.get_prompt(name, args)).messages[0].content.text.split())


async def fetched(call, tid):
    url = (await call("get_transfer_data_url", transfer_process_id=tid, connector="a"))["url"]
    async with httpx2.AsyncClient() as http:  # the link names host.docker.internal, which Docker Desktop resolves
        response = await http.get(url)
    assert response.status_code == 200, response.text
    return response.json()


async def test_consume_dataset_automatic_pull(http_client, call):
    steps = await prompt(http_client, "consume_dataset", provider="b", connector="a")

    assert 'browse_remote_catalog(peer="b", connector="a")' in steps
    dataset = (await call("browse_remote_catalog", peer="b", connector="a"))["datasets"][0]
    assert 'get_remote_dataset_formats(peer="b", dataset_id=<it>, connector="a")' in steps
    formats = await call("get_remote_dataset_formats", peer="b", dataset_id=dataset["id"], connector="a")
    assert "HttpData-PULL" in formats["formats"]

    assert 'request_contract(peer="b", dataset_id=<it>, offer_id=<it>, connector="a")' in steps
    offer_id = dataset["offers"][0]["id"]
    negotiation = await call("request_contract", peer="b", dataset_id=dataset["id"], offer_id=offer_id, connector="a")
    assert 'states=["OFFERED", "AGREED", "FINALIZED"], connector="a")' in steps
    waited = await call(
        "wait_for_state",
        kind="negotiation",
        id=negotiation["@id"],
        states=["OFFERED", "AGREED", "FINALIZED"],
        connector="a",
    )
    assert (waited["outcome"], waited["state"]) == ("reached", "FINALIZED")  # automatic negotiation
    agreement_id = waited["object"]["agreement"]["@id"]

    assert 'find_transfer_for_agreement(agreement_id=<A>, role="consumer", connector="a")' in steps
    transfer = await call("find_transfer_for_agreement", agreement_id=agreement_id, role="consumer", connector="a")
    assert 'request_transfer(transfer_process_id=<T>, format="HttpData-PULL", connector="a")' in steps
    await call("request_transfer", transfer_process_id=transfer["@id"], format="HttpData-PULL", connector="a")
    assert 'states=["STARTED", "DOWNLOADED"], connector="a")' in steps
    waited = await call(
        "wait_for_state", kind="transfer", id=transfer["@id"], states=["STARTED", "DOWNLOADED"], connector="a"
    )
    assert waited["outcome"] == "reached", waited
    if not waited["object"].get("downloaded"):  # STARTED came first: the prompt's step 5
        assert 'move_transfer_data(transfer_process_id=<T>, connector="a")' in steps
        moved = await call("move_transfer_data", transfer_process_id=transfer["@id"], connector="a")
        assert moved["accepted"] or "accepted=false means the data is already there or on its way" in steps
        waited = await call("wait_for_state", kind="transfer", id=transfer["@id"], states=["DOWNLOADED"], connector="a")
        assert waited["outcome"] == "reached", waited

    assert 'get_transfer_data_url(transfer_process_id=<T>, connector="a")' in steps
    assert await fetched(call, transfer["@id"]) == SEEDED_DATA


async def test_handle_incoming_negotiations_manual_push(http_client, call, manual_negotiation, manual_transfer):
    steps = await prompt(http_client, "handle_incoming_negotiations", connector="b")

    # The consumer's side, as consume_dataset does it on connector a.
    dataset = (await call("browse_remote_catalog", peer="b", connector="a"))["datasets"][0]
    mine = await call("request_contract", peer="b", dataset_id=dataset["id"], connector="a")
    pid = mine["consumerPid"]

    # Prompt steps 1 and 2: the waiting request, agreed by the provider. Filtering by consumerPid keeps other
    # runs' leftovers out.
    assert 'list_negotiations(role="provider", state=S, connector="b")' in steps
    [theirs] = (await call("list_negotiations", role="provider", state="REQUESTED", consumer_pid=pid, connector="b"))[
        "items"
    ]
    assert 'agree_negotiation(negotiation_id, connector="b")' in steps
    await call("agree_negotiation", negotiation_id=theirs["@id"], connector="b")

    await call("wait_for_state", kind="negotiation", id=mine["@id"], states=["AGREED"], connector="a")
    await call("verify_negotiation", negotiation_id=mine["@id"], connector="a")

    # Prompt step 3: the verified one is finalized.
    [verified] = (await call("list_negotiations", role="provider", state="VERIFIED", consumer_pid=pid, connector="b"))[
        "items"
    ]
    assert 'finalize_negotiation(negotiation_id, connector="b")' in steps
    await call("finalize_negotiation", negotiation_id=verified["@id"], connector="b")
    waited = await call("wait_for_state", kind="negotiation", id=mine["@id"], states=["FINALIZED"], connector="a")
    agreement_id = waited["object"]["agreement"]["@id"]

    transfer = await call("find_transfer_for_agreement", agreement_id=agreement_id, role="consumer", connector="a")
    await call("request_transfer", transfer_process_id=transfer["@id"], format="HttpData-PUSH", connector="a")

    # Prompt step 4: the requested transfer is started by the provider.
    assert 'list_transfers(role="provider", state="REQUESTED", connector="b")' in steps
    requested = await call(
        "list_transfers", role="provider", state="REQUESTED", agreement_id=agreement_id, connector="b"
    )
    [provider_transfer] = requested["items"]
    assert 'start_transfer(transfer_process_id, connector="b")' in steps
    await call("start_transfer", transfer_process_id=provider_transfer["@id"], connector="b")

    # Prompt step 5: PUSH data is moved by the provider.
    assert 'list_transfers(role="provider", state="STARTED", format="HttpData-PUSH", connector="b")' in steps
    started = await call(
        "list_transfers",
        role="provider",
        state="STARTED",
        format="HttpData-PUSH",
        agreement_id=agreement_id,
        connector="b",
    )
    assert [t["downloaded"] for t in started["items"]] == [False]
    assert 'move_transfer_data(transfer_process_id, connector="b")' in steps
    moved = await call("move_transfer_data", transfer_process_id=provider_transfer["@id"], connector="b")
    assert moved["accepted"] is True
    assert 'wait_for_state(kind="transfer", id=<it>, states=["DOWNLOADED"], connector="b")' in steps
    waited = await call(
        "wait_for_state", kind="transfer", id=provider_transfer["@id"], states=["DOWNLOADED"], connector="b"
    )
    assert waited["outcome"] == "reached", waited

    assert await fetched(call, transfer["@id"]) == SEEDED_DATA  # the consumer's copy
