
import pytest
import pytest_asyncio
from fakeredis import FakeStrictRedis

from hfa.runtime.lineage_store import LineageStore
from hfa.runtime.payload_store import LocalFilePayloadStore, PayloadIntegrityError
from hfa_worker.input_resolver import InputResolver


@pytest_asyncio.fixture
async def redis_client():
    client = FakeStrictRedis(decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()


@pytest.mark.asyncio
async def test_produced_output_is_run_scoped_and_first_write_wins(redis_client):
    store = LineageStore(redis_client)
    ok1 = await store.record_produced_output(
        run_id="run-1",
        task_id="task-A",
        output_ref="file://a",
        payload_mode="ref",
        payload_size=10,
        checksum="abc",
        payload_type="binary",
        producer_worker_id="worker-1",
        producer_attempt=1,
    )
    ok2 = await store.record_produced_output(
        run_id="run-2",
        task_id="task-A",
        output_ref="file://b",
        payload_mode="ref",
        payload_size=11,
        checksum="def",
        payload_type="binary",
        producer_worker_id="worker-2",
        producer_attempt=1,
    )
    ok3 = await store.record_produced_output(
        run_id="run-1",
        task_id="task-A",
        output_ref="file://c",
        payload_mode="ref",
        payload_size=12,
        checksum="ghi",
        payload_type="binary",
        producer_worker_id="worker-3",
        producer_attempt=2,
    )

    assert ok1 is True
    assert ok2 is True
    assert ok3 is False

    produced1 = await store.get_produced_output(run_id="run-1", task_id="task-A")
    produced2 = await store.get_produced_output(run_id="run-2", task_id="task-A")
    assert produced1["output_ref"] == "file://a"
    assert produced2["output_ref"] == "file://b"


@pytest.mark.asyncio
async def test_duplicate_consumer_lineage_does_not_expand(redis_client):
    store = LineageStore(redis_client)
    ok1 = await store.record_consumed_input(
        run_id="run-1",
        parent_task_id="task-A",
        consumer_task_id="task-B",
        consumed_output_ref="file://a",
        checksum="abc",
        consumer_worker_id="worker-9",
        consumer_attempt=1,
    )
    ok2 = await store.record_consumed_input(
        run_id="run-1",
        parent_task_id="task-A",
        consumer_task_id="task-B",
        consumed_output_ref="file://a",
        checksum="abc",
        consumer_worker_id="worker-9",
        consumer_attempt=1,
    )
    consumers = await store.get_consumers(run_id="run-1", task_id="task-A")
    assert ok1 is True
    assert ok2 is False
    assert len(consumers) == 1


@pytest.mark.asyncio
async def test_checksum_verified_before_consumer_lineage_write(redis_client, tmp_path):
    payload_store = LocalFilePayloadStore(str(tmp_path))
    data = b"abcdefghi"
    envelope = await payload_store.put(data)

    redis_client.set(
        "hfa:task:task-A:output",
        '{"payload_mode":"ref","payload_ref":"%s","payload_type":"binary","checksum":"wrong"}' % envelope.payload_ref,
    )

    store = LineageStore(redis_client)
    resolver = InputResolver(redis_client, payload_store=payload_store, lineage_store=store)

    with pytest.raises(PayloadIntegrityError):
        await resolver.resolve_input(
            "task-A",
            run_id="run-1",
            consumer_task_id="task-B",
            consumer_worker_id="worker-9",
            consumer_attempt=1,
        )

    consumers = await store.get_consumers(run_id="run-1", task_id="task-A")
    assert consumers == []
