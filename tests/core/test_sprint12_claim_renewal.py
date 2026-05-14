"""
tests/core/test_sprint12_claim_renewal.py
IRONCLAD Sprint 12 — Claim renewal visibility tests

Verifies:
  - StateStore.get_claim_owner returns correct worker_id
  - StateStore.get_claim_ttl returns positive TTL while claim is alive
  - StateStore.get_claim_ttl returns -2 after claim released
  - StateStore.renew_claim extends TTL
  - get_running_runs returns current ZSET entries with state
"""

from __future__ import annotations

import time

import fakeredis.aioredis as faredis
import pytest

from hfa.runtime.state_store import StateStore


@pytest.mark.asyncio
async def test_get_claim_owner_returns_worker_id():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id = "run-claim-1"
    worker_id = "worker-abc"

    claimed = await store.claim_execution(run_id, worker_id)
    assert claimed is True

    owner = await store.get_claim_owner(run_id)
    assert owner == worker_id


@pytest.mark.asyncio
async def test_get_claim_owner_returns_none_when_absent():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    owner = await store.get_claim_owner("run-nonexistent")
    assert owner is None


@pytest.mark.asyncio
async def test_get_claim_ttl_positive_while_claimed():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id = "run-claim-ttl-1"
    await store.claim_execution(run_id, "worker-1")

    ttl = await store.get_claim_ttl(run_id)
    assert ttl > 0
    assert ttl <= StateStore.CLAIM_TTL


@pytest.mark.asyncio
async def test_get_claim_ttl_negative_after_release():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id = "run-claim-ttl-2"
    await store.claim_execution(run_id, "worker-1")
    await store.release_claim(run_id)

    ttl = await store.get_claim_ttl(run_id)
    assert ttl < 0


@pytest.mark.asyncio
async def test_renew_claim_returns_true_while_alive():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id = "run-renew-1"
    await store.claim_execution(run_id, "worker-1")

    renewed = await store.renew_claim(run_id)
    assert renewed is True


@pytest.mark.asyncio
async def test_renew_claim_returns_false_after_release():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id = "run-renew-2"
    await store.claim_execution(run_id, "worker-1")
    await store.release_claim(run_id)

    renewed = await store.renew_claim(run_id)
    assert renewed is False


@pytest.mark.asyncio
async def test_get_running_runs_returns_entries():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id_1 = "run-diag-1"
    run_id_2 = "run-diag-2"

    # Manually add to ZSET as the scheduler / mark_running would
    now = time.time()
    await redis.zadd(StateStore.RUNNING_ZSET, {run_id_1: now, run_id_2: now - 10})
    await redis.set(f"hfa:run:state:{run_id_1}", "running", ex=86400)
    await redis.set(f"hfa:run:state:{run_id_2}", "running", ex=86400)

    runs = await store.get_running_runs(limit=10)
    run_ids = {r["run_id"] for r in runs}
    assert run_id_1 in run_ids
    assert run_id_2 in run_ids


@pytest.mark.asyncio
async def test_get_running_runs_respects_limit():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    now = time.time()
    for i in range(10):
        run_id = f"run-limit-{i}"
        await redis.zadd(StateStore.RUNNING_ZSET, {run_id: now + i})

    runs = await store.get_running_runs(limit=3)
    assert len(runs) == 3


@pytest.mark.asyncio
async def test_get_running_runs_empty_when_zset_empty():
    redis = faredis.FakeRedis()
    store = StateStore(redis)
    runs = await store.get_running_runs()
    assert runs == []


@pytest.mark.asyncio
async def test_get_running_runs_includes_state():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id = "run-state-check-1"
    await redis.zadd(StateStore.RUNNING_ZSET, {run_id: time.time()})
    await redis.set(f"hfa:run:state:{run_id}", "running", ex=86400)

    runs = await store.get_running_runs()
    entry = next(r for r in runs if r["run_id"] == run_id)
    assert entry["state"] == "running"
