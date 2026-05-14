
from __future__ import annotations

import hashlib
import pytest

from hfa.runtime.lineage_store import LineageStore
from hfa.runtime.payload_store import LocalFilePayloadStore, PayloadIntegrityError
from hfa.runtime.state_store import StateStore
from hfa_worker.input_resolver import InputResolver


pytestmark = pytest.mark.asyncio


class _Lua:
    async def execute(self, *args, **kwargs):
        return 1


class _EventStore:
    def __init__(self):
        self.calls = []

    async def append_event(self, run_id, event_type, worker_id=None, details=None):
        self.calls.append((run_id, event_type, worker_id, details))
        return True


@pytest.fixture
def event_store():
    return _EventStore()


@pytest.fixture
def lineage_store(redis_client):
    return LineageStore(redis_client)


@pytest.fixture
def payload_store(tmp_path):
    return LocalFilePayloadStore(str(tmp_path))


@pytest.fixture
def state_store(redis_client, event_store, payload_store, lineage_store):
    return StateStore(
        redis_client,
        _Lua(),
        event_store=event_store,
        payload_store=payload_store,
        lineage_store=lineage_store,
    )


@pytest.fixture
def input_resolver(redis_client, payload_store, lineage_store):
    return InputResolver(
        redis_client,
        payload_store=payload_store,
        lineage_store=lineage_store,
    )


async def test_produced_lineage_is_written_after_output_persistence(
    state_store,
    lineage_store,
):
    payload = b"hello-lineage"
    checksum = hashlib.sha256(payload).hexdigest()

    record = await state_store.store_task_output(
        task_id="task-A",
        run_id="run-1",
        payload=payload,
        worker_id="worker-1",
        attempt=1,
    )

    produced = await lineage_store.get_produced_output(run_id="run-1", task_id="task-A")

    assert record["checksum"] == checksum
    assert produced is not None
    assert produced["run_id"] == "run-1"
    assert produced["task_id"] == "task-A"
    assert produced["checksum"] == checksum
    assert produced["producer_worker_id"] == "worker-1"
    assert produced["producer_attempt"] == 1


async def test_consumer_lineage_and_edge_graph_are_recorded_on_resolve(
    state_store,
    input_resolver,
    lineage_store,
):
    payload = b"payload-for-child"

    await state_store.store_task_output(
        task_id="task-parent",
        run_id="run-2",
        payload=payload,
        worker_id="worker-parent",
        attempt=1,
    )

    resolved = await input_resolver.resolve_input(
        "task-parent",
        run_id="run-2",
        consumer_task_id="task-child",
        consumer_worker_id="worker-child",
        consumer_attempt=1,
    )

    consumers = await lineage_store.get_consumers(run_id="run-2", task_id="task-parent")
    edges = await lineage_store.get_run_edges(run_id="run-2")

    assert resolved.checksum is not None
    assert len(consumers) == 1
    assert consumers[0]["consumer_task_id"] == "task-child"
    assert consumers[0]["consumer_worker_id"] == "worker-child"
    assert consumers[0]["checksum"] == resolved.checksum

    assert len(edges) == 1
    assert edges[0]["parent_task_id"] == "task-parent"
    assert edges[0]["child_task_id"] == "task-child"


async def test_duplicate_consume_does_not_expand_lineage(
    state_store,
    input_resolver,
    lineage_store,
):
    await state_store.store_task_output(
        task_id="task-src",
        run_id="run-3",
        payload=b"same-data",
        worker_id="worker-1",
        attempt=1,
    )

    await input_resolver.resolve_input(
        "task-src",
        run_id="run-3",
        consumer_task_id="task-dst",
        consumer_worker_id="worker-2",
        consumer_attempt=1,
    )
    await input_resolver.resolve_input(
        "task-src",
        run_id="run-3",
        consumer_task_id="task-dst",
        consumer_worker_id="worker-2",
        consumer_attempt=1,
    )

    consumers = await lineage_store.get_consumers(run_id="run-3", task_id="task-src")
    edges = await lineage_store.get_run_edges(run_id="run-3")

    assert len(consumers) == 1
    assert len(edges) == 1


async def test_checksum_mismatch_blocks_consumer_lineage(
    redis_client,
    payload_store,
    lineage_store,
    input_resolver,
):
    data = b"abcdefghi"
    envelope = await payload_store.put(data)

    corrupt_record = {
        "payload_mode": "ref",
        "payload_inline": None,
        "payload_ref": envelope.payload_ref,
        "payload_type": "binary",
        "payload_size": envelope.payload_size,
        "checksum": "wrong-checksum",
        "produced_by": {"worker_id": "worker-1", "attempt": 1},
        "run_id": "run-4",
    }

    import json
    await redis_client.set("hfa:task:task-bad:output", json.dumps(corrupt_record))

    with pytest.raises(PayloadIntegrityError):
        await input_resolver.resolve_input(
            "task-bad",
            run_id="run-4",
            consumer_task_id="task-child",
            consumer_worker_id="worker-2",
            consumer_attempt=1,
        )

    consumers = await lineage_store.get_consumers(run_id="run-4", task_id="task-bad")
    edges = await lineage_store.get_run_edges(run_id="run-4")

    assert consumers == []
    assert edges == []


async def test_run_isolation_for_same_task_id(
    state_store,
    lineage_store,
):
    await state_store.store_task_output(
        task_id="task-same",
        run_id="run-A",
        payload=b"A-data",
        worker_id="worker-A",
        attempt=1,
    )
    await state_store.store_task_output(
        task_id="task-same",
        run_id="run-B",
        payload=b"B-data",
        worker_id="worker-B",
        attempt=1,
    )

    produced_a = await lineage_store.get_produced_output(run_id="run-A", task_id="task-same")
    produced_b = await lineage_store.get_produced_output(run_id="run-B", task_id="task-same")

    assert produced_a is not None
    assert produced_b is not None
    assert produced_a["run_id"] == "run-A"
    assert produced_b["run_id"] == "run-B"
    assert produced_a["checksum"] != produced_b["checksum"]
