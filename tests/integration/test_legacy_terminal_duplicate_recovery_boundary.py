"""Separate negative census: canonical recovery ACK remains fail-closed."""
from types import SimpleNamespace

import pytest

from hfa.config.keys import RedisKey
from hfa_worker.consumer import CONSUMER_GROUP
import hfa_worker.run_finalizing_runtime as runtime
from tests.integration.test_terminal_task_run_finalization import (
    _canonical_terminal_duplicate_case,
    _dag,
    _pending_count,
    _result_events,
    _seed_run,
    _terminal_duplicate_delivery,
    alpha,
    root_redis,
)
from tests.unit.test_f04_4c_task_claim_projection_proof import snapshot
from tests.unit.test_f04_task_projection_proof import write_counts


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_proofless_legacy_recovery_finalizes_but_cannot_ack(redis_client, monkeypatch):
    run_id = "run-terminal-duplicate-finalize"
    tenant_id = "tenant-terminal-duplicate-finalize"
    task_id = "task-terminal-duplicate-finalize"
    await _seed_run(
        redis_client, run_id=run_id, tenant_id=tenant_id, tasks={task_id: "done"},
    )
    case = await _terminal_duplicate_delivery(
        redis_client, await _dag(redis_client),
        run_id=run_id, tenant_id=tenant_id, task_id=task_id,
        worker_id="worker-terminal-duplicate-finalize",
        stream="hfa:sprint83:terminal-duplicate",
    )
    statuses = []
    actual_ack = runtime.ack_message

    async def observe_ack(redis, stream, group, message_id, *, ctx, run_termination):
        assert run_termination.finalized and run_termination.ack_allowed
        assert not getattr(run_termination, "canonical_head_keys", ())
        assert not getattr(run_termination, "canonical_head_witness", ())
        before, writes = await snapshot(redis), await write_counts(redis)
        status = await actual_ack(
            redis, stream, group, message_id, ctx=ctx, run_termination=run_termination,
        )
        assert status == "recovery_ack_proof_conflict"
        assert await snapshot(redis) == before
        assert await write_counts(redis) == writes
        statuses.append(status)
        return status

    monkeypatch.setattr(runtime, "ack_message", observe_ack)
    await case.consumer._process_message_via_task_consumer(
        case.event, case.message_id, case.stream, shard=0,
    )
    assert statuses == ["recovery_ack_proof_conflict"]
    assert await _pending_count(redis_client, case.stream) == 1
    assert await redis_client.get(RedisKey.run_state(run_id)) == "done"
    assert len(await _result_events(redis_client, run_id)) == 1
    case.executor.execute.assert_not_awaited()


@pytest.mark.parametrize("fault", [
    "missing_receipt", "stale_head", "foreign_run",
    "transport_run_id", "transport_task_id", "transport_tenant_id",
])
async def test_invalid_canonical_proof_or_transport_retains_pending(
    redis_client, alpha, monkeypatch, fault,
):
    case = await _canonical_terminal_duplicate_case(redis_client, alpha)
    foreign_result = None
    if fault == "foreign_run":
        foreign = await _canonical_terminal_duplicate_case(redis_client, alpha)
        foreign_result = await foreign.binding.terminate(
            run_id=foreign.req.run_id, tenant_id=foreign.req.tenant_id,
            trigger_task_id=foreign.req.root_task_id, finalized_at_ms=6000,
            worker_instance_id="worker-1", trigger_terminal_state="done",
        )
        assert foreign_result.finalized and foreign_result.canonical_head_witness
        foreign.executor.execute.assert_not_awaited()
    actual_ack = runtime.ack_message
    statuses = []

    async def observe_ack(redis, stream, group, message_id, *, ctx, run_termination):
        assert run_termination.canonical_head_keys and run_termination.canonical_head_witness
        if fault == "missing_receipt":
            await redis.hdel(run_termination.canonical_head_keys[2], run_termination.canonical_head_witness[2])
        elif fault == "stale_head":
            await redis.hset(run_termination.canonical_head_keys[0], mapping=case.prior_run_head)
        elif fault == "foreign_run":
            # This is another real accepted RUN result, never a fabricated witness.
            run_termination = foreign_result
        else:
            identity = {key: getattr(ctx, key) for key in ("run_id", "tenant_id", "task_id")}
            identity[fault.removeprefix("transport_")] = "foreign-transport-identity"
            ctx = SimpleNamespace(**identity)
        # Fault injection is the precondition; the actual ACK must mutate nothing.
        before, writes = await snapshot(redis), await write_counts(redis)
        status = await actual_ack(
            redis, stream, group, message_id, ctx=ctx, run_termination=run_termination,
        )
        assert status == "recovery_ack_proof_conflict"
        assert await snapshot(redis) == before
        assert await write_counts(redis) == writes
        statuses.append(status)
        return status

    monkeypatch.setattr(runtime, "ack_message", observe_ack)
    task_head = await case.store.get_aggregate_snapshot(case.task)
    await case.consumer._process_message_via_task_consumer(
        case.event, case.message_id, case.stream, shard=0,
    )
    assert statuses == ["recovery_ack_proof_conflict"]
    assert await _pending_count(redis_client, case.stream) == 1
    pending = await redis_client.xpending_range(case.stream, CONSUMER_GROUP, "-", "+", 10)
    assert [entry["message_id"] for entry in pending] == [case.message_id]
    assert await case.store.get_aggregate_snapshot(case.task) == task_head
    assert len(await _result_events(redis_client, case.req.run_id)) == 1
    case.executor.execute.assert_not_awaited()
