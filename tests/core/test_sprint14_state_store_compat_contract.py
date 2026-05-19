import time

import fakeredis.aioredis as faredis
import pytest

from hfa.runtime.state_store import StateStore


@pytest.mark.asyncio
async def test_state_store_worker_compatibility_contract():
    redis = faredis.FakeRedis()
    store = StateStore(redis)

    run_id = "compat-run-1"
    tenant_id = "tenant-compat"

    await store.create_run_meta(
        run_id,
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
            "reschedule_count": "0",
            "admitted_at": str(time.time()),
            "state": "scheduled",
        },
    )

    assert await store.get_run_state(run_id) == "scheduled"
    assert await store.is_terminal(run_id) is False

    claimed = await store.mark_running(run_id, "worker-1", "group-a", 0)
    assert claimed is True
    assert await store.get_claim_owner(run_id) == "worker-1"
    assert await store.renew_claim(run_id) is True

    meta = await store.get_run_meta(run_id)
    assert meta["worker_id"] == "worker-1"
    assert meta["worker_group"] == "group-a"
    assert meta["state"] == "running"

    await store.store_result(
        run_id,
        tenant_id,
        "done",
        {"out": "ok"},
        cost_cents=7,
        tokens_used=11,
    )

    result = await store.get_result(run_id)
    assert result is not None
    assert result["status"] == "done"
    assert result["payload"] == {"out": "ok"}
    assert result["cost_cents"] == 7
    assert result["tokens_used"] == 11
    assert result["completed_at"] > 0

    await store.mark_completed(run_id)
    assert await store.is_terminal(run_id) is True
    assert await store.get_claim_owner(run_id) is None
