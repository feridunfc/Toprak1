
import asyncio
import pytest
import pytest_asyncio
from fakeredis import FakeStrictRedis

from hfa.runtime.payload_metrics import reset_all, snapshot
from hfa.runtime.payload_store import LocalFilePayloadStore, PayloadIntegrityError
from hfa.runtime.state_store import StateStore
from hfa_worker.input_resolver import InputResolver


class _Lua:
    async def execute(self, *args, **kwargs):
        return 1


@pytest_asyncio.fixture
async def redis_client():
    client = FakeStrictRedis(decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()


@pytest.mark.asyncio
async def test_small_payload_inline(redis_client, monkeypatch):
    monkeypatch.setenv("INLINE_THRESHOLD_BYTES", "1024")
    reset_all()
    store = StateStore(redis_client, _Lua(), payload_store=LocalFilePayloadStore())
    payload = b"hello"
    record = await store.store_task_output(task_id="t1", run_id="r1", payload=payload, worker_id="w1", attempt=1)
    assert record["payload_mode"] == "inline"
    assert record["payload_inline"] == "hello"
    assert snapshot()["payload_inline_count"] == 1


@pytest.mark.asyncio
async def test_large_payload_ref_roundtrip(redis_client, monkeypatch, tmp_path):
    monkeypatch.setenv("INLINE_THRESHOLD_BYTES", "4")
    reset_all()
    payload_store = LocalFilePayloadStore(str(tmp_path))
    store = StateStore(redis_client, _Lua(), payload_store=payload_store)
    payload = b"abcdefghi"
    record = await store.store_task_output(task_id="t2", run_id="r2", payload=payload, worker_id="w1", attempt=1)
    assert record["payload_mode"] == "ref"
    resolver = InputResolver(redis_client, payload_store=payload_store)
    resolved = await resolver.resolve_input("t2")
    assert resolved == payload


@pytest.mark.asyncio
async def test_checksum_validation_detects_corruption(redis_client, monkeypatch, tmp_path):
    monkeypatch.setenv("INLINE_THRESHOLD_BYTES", "4")
    payload_store = LocalFilePayloadStore(str(tmp_path))
    store = StateStore(redis_client, _Lua(), payload_store=payload_store)
    payload = b"abcdefghi"
    record = await store.store_task_output(task_id="t3", run_id="r3", payload=payload, worker_id="w1", attempt=1)
    ref = record["payload_ref"]
    path = ref[len("file://"):]
    with open(path, "wb") as f:
        f.write(b"corrupted")
    resolver = InputResolver(redis_client, payload_store=payload_store)
    with pytest.raises(PayloadIntegrityError):
        await resolver.resolve_input("t3")


@pytest.mark.asyncio
async def test_duplicate_guard_skips_payload_rewrite(redis_client, tmp_path):
    payload_store = LocalFilePayloadStore(str(tmp_path))
    store = StateStore(redis_client, _Lua(), payload_store=payload_store)
    result = await store.store_task_output(
        task_id="t4",
        run_id="r4",
        payload=b"abc",
        worker_id="w1",
        attempt=1,
        duplicate_guard=True,
    )
    assert result["skipped"] is True


@pytest.mark.asyncio
async def test_concurrent_large_payload_roundtrip(redis_client, monkeypatch, tmp_path):
    monkeypatch.setenv("INLINE_THRESHOLD_BYTES", "4")
    payload_store = LocalFilePayloadStore(str(tmp_path))
    store = StateStore(redis_client, _Lua(), payload_store=payload_store)

    async def writer(i: int):
        payload = f"payload-{i}-" * 500
        return await store.store_task_output(
            task_id=f"task-{i}",
            run_id=f"run-{i}",
            payload=payload.encode(),
            worker_id="w1",
            attempt=1,
        )

    records = await asyncio.gather(*(writer(i) for i in range(5)))
    resolver = InputResolver(redis_client, payload_store=payload_store)
    for i, record in enumerate(records):
        assert record["payload_mode"] == "ref"
        resolved = await resolver.resolve_input(f"task-{i}")
        assert resolved == (f"payload-{i}-" * 500).encode()
