"""R4P: real WorkerService -> coordinator -> canonical proof -> atomic ACK.

Uses the accepted exclusively owned Redis fixtures and explicit alpha activation.
The completion manager, finalizer, authority result and ACK are never facades.
"""
import asyncio
from dataclasses import dataclass, replace
import json
from typing import Any

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader
from hfa_control.run_terminal_event_evidence import backfill_terminal_event_evidence
from hfa_control.run_terminate_authority import run_terminate_identity
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService
from hfa_worker.recovery_ack import ack_message as actual_ack
from hfa_worker.redis_utils import ensure_consumer_group
import hfa_worker.run_finalizing_runtime as runtime
from tests.integration.test_f02a1_root_binding_integration import (
    root_redis, flush_redis, integration_redis_stack,
)
from tests.unit.test_f02a1_root_binding import alpha
from tests.unit.test_f04_4c_task_claim_projection_proof import claim, setup, snapshot
from tests.unit.test_f04_task_projection_proof import write_counts
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]
PROOF_FIELDS = (
    "canonical_transition_id", "canonical_record_hash", "canonical_command_hash",
    "canonical_revision", "terminal_proof_sha256", "canonical_head_keys", "canonical_head_witness",
)


@dataclass
class RecoveryCase:
    redis: Any
    req: Any
    store: Any
    task: Any
    worker: WorkerService
    stream: str
    message_id: str
    message: dict

    async def deliver(self):
        await self.worker._consumer._process_message(self.message_id, self.message, self.stream, 2)

    async def pending(self):
        return len(await self.redis.xpending_range(
            self.stream, CONSUMER_GROUP, self.message_id, self.message_id, 1,
        ))


def worker(redis, worker_id="worker-1", **overrides):
    config = dict(
        production=True, product_mode="SINGLE_TASK_ALPHA", worker_id=worker_id,
        worker_group="group-1", region="integration", shards=[2], capacity=1,
        executor_mode="fake", run_termination_binding_enabled=True,
        canonical_task_admit_binding=True, canonical_task_dispatch_binding=True,
        canonical_task_claim_binding=True, canonical_task_terminal_binding=True,
        canonical_resource_settlement_binding=True, reclaim_idle_ms=0,
    )
    config.update(overrides)
    service = WorkerService(redis, config)
    coordinator = service._run_termination_coordinator
    assert service._task_consumer._completion_manager is coordinator
    assert coordinator._authority_binding is service._run_terminate_authority_binding
    assert coordinator._task_completion_gateway is service._task_terminal_completion_gateway
    assert service._consumer._task_consumer is service._task_consumer
    assert service._consumer._task_terminal_authority_binding is service._task_terminal_authority_binding
    assert service._run_terminate_authority_binding.resource_manager is service._resource_settlement_manager
    return service


async def initialize(service):
    await service._dag_lua.initialise()
    await service._task_terminal_authority_binding.initialise()
    await service._run_termination_coordinator.initialise()


async def terminal_case(alpha, redis, state="done", *, project_task=True):
    req, store, dag, item, task, manager = await setup(alpha, redis)
    if state != "scheduled":
        assert (await claim(manager, item)).ok
    service = worker(redis)
    await initialize(service)
    terminal = service._task_terminal_authority_binding
    args = dict(task_id=item.task_id, run_id=item.run_id, tenant_id=item.tenant_id,
                finished_at_ms=4000, worker_instance_id="worker-1", scheduler_epoch="epoch-7",
                claim_epoch=1)
    if state in {"scheduled", "running"}:
        pass
    elif project_task:
        result = (await terminal.complete(**args, output_data='{"once":true}') if state == "done"
                  else await terminal.fail(**args, reason_code="failed"))
        assert result.completed and result.projection_applied
    else:
        # Prepare/commit genuine TASK authority; deliberately stop before projection.
        prepared = await terminal.prepare_terminal(
            **args, terminal_state=state, reason_code="completed" if state == "done" else "failed",
            output_data='{"once":true}' if state == "done" else "",
        )
        assert prepared.projection.canonical_revision == 4
        assert await redis.get(DagRedisKey.task_state(req.root_task_id)) == "running"
    await ensure_consumer_group(redis, RedisKey.stream_results(), "r4p-evidence-setup", start_id="0", mkstream=True)
    await backfill_terminal_event_evidence(redis, completed_at_ms=5000)
    stream = RedisKey.stream_shard(2)
    await ensure_consumer_group(redis, stream, CONSUMER_GROUP, start_id="0", mkstream=True)
    pending_before = (await redis.xpending(stream, CONSUMER_GROUP))["pending"]
    rows = await redis.xreadgroup(CONSUMER_GROUP, "previous-worker", {stream: ">"}, count=1)
    assert len(rows) == 1 and len(rows[0][1]) == 1
    message_id, message = rows[0][1][0]
    assert message["run_id"] == req.run_id and message["task_id"] == req.root_task_id
    assert message["tenant_id"] == req.tenant_id
    assert (await redis.xpending(stream, CONSUMER_GROUP))["pending"] == pending_before + 1
    case = RecoveryCase(redis, req, store, task, service, stream, message_id, message)
    assert await case.pending() == 1
    return case


def observe(case, monkeypatch):
    """Only observe/delegate; never supply a replacement positive result."""
    seen = {key: [] for key in ("binding", "coordinator", "ack", "lua", "settle", "execute", "claim", "task")}
    service = case.worker
    async def record_call(name, original, *args, **kwargs):
        attempt = name in {"execute", "claim", "task"}
        if attempt:
            seen[name].append((args, kwargs))
        value = await original(*args, **kwargs)
        if not attempt:
            seen[name].append(value)
        return value
    for name, obj, method in (
        ("binding", service._run_terminate_authority_binding, "terminate"),
        ("coordinator", service._run_termination_coordinator, "finalize_run_from_tasks"),
        ("settle", service._resource_settlement_manager, "settle_once"),
        ("execute", service._task_consumer._executor, "execute"),
        ("claim", service._task_claim_manager, "claim_start"),
        ("task", service._task_terminal_completion_gateway, "task_complete"),
    ):
        original = getattr(obj, method)
        async def wrapped(*args, _name=name, _original=original, **kwargs):
            return await record_call(_name, _original, *args, **kwargs)
        monkeypatch.setattr(obj, method, wrapped)
    async def ack(redis, stream, group, message_id, *, ctx, run_termination):
        status = await actual_ack(redis, stream, group, message_id, ctx=ctx, run_termination=run_termination)
        seen["ack"].append((status, run_termination, stream, group, message_id))
        return status
    monkeypatch.setattr(runtime, "ack_message", ack)
    original_run = LuaScriptLoader.run
    async def lua(loader, **kwargs):
        result = await original_run(loader, **kwargs)
        if loader._path.name == "worker_recovery_ack.lua":
            seen["lua"].append((kwargs, result))
        return result
    monkeypatch.setattr(LuaScriptLoader, "run", lua)
    return seen


async def assert_effects_once(case, seen, state):
    head = await case.store.get_aggregate_snapshot(run_terminate_identity(case.req.run_id))
    assert head.revision == 2 and head.state == state
    probe = await case.store.load_receipt_probe(run_terminate_identity(case.req.run_id), head.operation_id)
    assert probe.canonical_store_record.operation_type == "RUN_TERMINATE"
    keys = case.store.keyspace(head.canonical_aggregate_identity_sha256).commit_keys()[:6]
    assert [await case.redis.hlen(key) for key in keys[1:4]] == [2, 2, 2]
    assert [await case.redis.xlen(key) for key in keys[4:]] == [2, 2]
    events = [row for _, row in await case.redis.xrange(RedisKey.stream_results()) if row.get("run_id") == case.req.run_id]
    assert len(events) == 1 and events[0]["event_type"] == ("RunCompleted" if state == "done" else "RunFailed")
    assert not seen["execute"] and not seen["claim"] and not seen["task"]
    assert sum(value.status == "settled" for value in seen["settle"]) == 1
    assert await case.worker._resource_settlement_manager.get_resource_snapshot(case.req.tenant_id) == {
        "concurrent_runs": 0, "budget_reserved_cents": 0, "tenant_inflight": 0,
    }


@pytest.mark.parametrize("state", ["done", "failed"])
async def test_real_worker_service_preserves_accepted_proof_then_acks_once(alpha, root_redis, monkeypatch, state):
    case = await terminal_case(alpha, root_redis, state)
    task_before = await case.store.get_aggregate_snapshot(case.task)
    seen = observe(case, monkeypatch)
    await case.deliver()
    print("R4P continuity", json.dumps({
        "pending": await case.pending(), "ack_statuses": [row[0] for row in seen["ack"]],
        "binding_has_proof": bool(seen["binding"] and seen["binding"][0].canonical_head_witness),
        "coordinator_has_proof": bool(seen["coordinator"] and getattr(seen["coordinator"][0], "canonical_head_witness", ())),
    }))
    assert await case.pending() == 0
    assert (await root_redis.xpending(case.stream, CONSUMER_GROUP))["pending"] == 0
    assert [row[0] for row in seen["ack"]] == ["acked"]
    accepted, carried = seen["binding"][0], seen["coordinator"][0]
    assert all(getattr(carried, field) == getattr(accepted, field) for field in PROOF_FIELDS)
    assert carried.canonical_head_witness is accepted.canonical_head_witness
    assert seen["ack"][0][1] is carried
    wire, raw = seen["lua"][0]
    assert wire["keys"] == [case.stream, *accepted.canonical_head_keys]
    assert wire["args"] == [json.dumps(accepted.canonical_head_witness, separators=(",", ":")),
        case.req.run_id, case.req.tenant_id, case.req.root_task_id, CONSUMER_GROUP, case.message_id,
        state, accepted.canonical_transition_id, accepted.canonical_record_hash,
        accepted.canonical_command_hash, str(accepted.canonical_revision), accepted.terminal_proof_sha256]
    assert raw == ["acked", "1"]
    assert await case.store.get_aggregate_snapshot(case.task) == task_before
    await assert_effects_once(case, seen, state)
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    await case.deliver()
    assert [row[0] for row in seen["ack"]] == ["acked", "already_acked"]
    assert seen["lua"][-1][1] == ["already_acked", "0"]
    assert all(getattr(seen["binding"][-1], field) == getattr(accepted, field) for field in PROOF_FIELDS)
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == writes
    await assert_effects_once(case, seen, state)


@pytest.mark.parametrize("state", ["done", "failed"])
async def test_task_projection_recovery_preserves_original_claim_owner(alpha, root_redis, monkeypatch, state):
    case = await terminal_case(alpha, root_redis, state, project_task=False)
    case.worker = worker(root_redis, "recovering-worker")
    await initialize(case.worker)
    task_before = await case.store.get_aggregate_snapshot(case.task)
    seen = observe(case, monkeypatch)
    await case.deliver()
    assert await case.pending() == 0 and [row[0] for row in seen["ack"]] == ["acked"]
    assert await case.store.get_aggregate_snapshot(case.task) == task_before
    assert await root_redis.get(DagRedisKey.task_state(case.req.root_task_id)) == state
    assert await root_redis.hget(DagRedisKey.task_meta(case.req.root_task_id), "worker_instance_id") == "worker-1"
    assert all(getattr(seen["binding"][0], field) == getattr(seen["coordinator"][0], field) for field in PROOF_FIELDS)
    await assert_effects_once(case, seen, state)


@pytest.mark.parametrize("failure_point", ["settlement", "projection"])
@pytest.mark.parametrize("state", ["done", "failed"])
async def test_retry_uses_durable_run_after_failed_effect(alpha, root_redis, monkeypatch, failure_point, state):
    case = await terminal_case(alpha, root_redis, state)
    seen = observe(case, monkeypatch)
    manager = (case.worker._resource_settlement_manager if failure_point == "settlement"
               else case.worker._run_terminate_authority_binding.projection_manager)
    method = "settle_once" if failure_point == "settlement" else "project"
    async def crash(*args, **kwargs):
        raise RuntimeError("controlled R4P effect failure")
    with monkeypatch.context() as fault:
        fault.setattr(manager, method, crash)
        await case.deliver()
    before = await case.store.get_aggregate_snapshot(run_terminate_identity(case.req.run_id))
    assert before.revision == 2 and before.state == state and await case.pending() == 1
    assert not seen["ack"] and not seen["coordinator"]
    case.worker = worker(root_redis, "restart-worker")
    await initialize(case.worker)
    retried = observe(case, monkeypatch)
    await case.deliver()
    assert await case.pending() == 0 and retried["ack"][0][0] == "acked"
    assert await case.store.get_aggregate_snapshot(run_terminate_identity(case.req.run_id)) == before
    assert not seen["execute"] and not retried["execute"]
    all_seen = {key: seen[key] + retried[key] for key in seen}
    await assert_effects_once(case, all_seen, state)


async def test_concurrent_service_recovery_has_one_ack_effect(alpha, root_redis, monkeypatch):
    case = await terminal_case(alpha, root_redis)
    # Establish genuine accepted finalization via the service coordinator, before ACK.
    await case.worker._run_termination_coordinator.finalize_run_from_tasks(
        run_id=case.req.run_id, tenant_id=case.req.tenant_id,
        trigger_task_id=case.req.root_task_id, finalized_at_ms=6000,
    )
    seen = observe(case, monkeypatch)
    writes = await write_counts(root_redis)
    await asyncio.gather(case.deliver(), case.deliver())
    assert sorted(row[0] for row in seen["ack"]) == ["acked", "already_acked"]
    assert sorted(tuple(raw) for _, raw in seen["lua"]) == [("acked", "1"), ("already_acked", "0")]
    assert await case.pending() == 0
    after = await write_counts(root_redis)
    assert {key: after[key] - value for key, value in writes.items() if after[key] != value} == {"xack": 1}
    assert not seen["execute"] and not seen["claim"] and not seen["task"]


async def test_service_start_reclaims_actual_pending_delivery(alpha, root_redis, monkeypatch):
    case = await terminal_case(alpha, root_redis)
    case.worker = worker(root_redis, "restart-lifecycle")
    seen = observe(case, monkeypatch)
    acked = asyncio.Event()
    observed_ack = runtime.ack_message
    async def notify(*args, **kwargs):
        value = await observed_ack(*args, **kwargs)
        if value == "acked":
            acked.set()
        return value
    monkeypatch.setattr(runtime, "ack_message", notify)
    try:
        await case.worker.start()
        await asyncio.wait_for(acked.wait(), 5)
        assert case.worker.is_ready and await case.pending() == 0
        await assert_effects_once(case, seen, "done")
    finally:
        await case.worker.stop(drain_timeout=1)
    assert not case.worker.is_ready
    assert case.worker._consumer._task is None


SCALAR_FIELDS = PROOF_FIELDS[:5] + ("final_state",)
ACK_FAULTS = (
    ["empty:" + field for field in PROOF_FIELDS]
    + ["drift:" + field for field in SCALAR_FIELDS]
    + ["key:" + str(index) for index in range(6)]
    + ["witness-shape", "witness-order", "foreign-run", "run-create", "stale-head"]
    + ["ctx:" + field for field in ("run_id", "task_id", "tenant_id")]
    + ["entry:" + field for field in ("run_id", "task_id", "tenant_id")]
    + ["duplicate-entry-field", "invalid-message-id", "trimmed-entry", "missing-group",
       "missing-stream", "wrong-stream-type"]
    + ["stored:" + fault for fault in FAULTS]
)


@pytest.mark.parametrize("fault", ACK_FAULTS)
async def test_actual_service_ack_rejects_proof_and_transport_faults(alpha, root_redis, monkeypatch, fault):
    case = await terminal_case(alpha, root_redis)
    run = run_terminate_identity(case.req.run_id)
    prior = await root_redis.hgetall(case.store.keyspace(run.sha256).aggregate)
    foreign_result = None
    if fault == "foreign-run":
        foreign = await terminal_case(alpha, root_redis)
        foreign_result = await foreign.worker._run_termination_coordinator.finalize_run_from_tasks(
            run_id=foreign.req.run_id, tenant_id=foreign.req.tenant_id,
            trigger_task_id=foreign.req.root_task_id, finalized_at_ms=6000,
        )
        assert foreign_result.canonical_head_witness
    seen = observe(case, monkeypatch)
    original_ack = runtime.ack_message
    boundaries = []
    async def damaged_ack(redis, stream, group, message_id, *, ctx, run_termination):
        assert all(getattr(run_termination, field) == getattr(seen["binding"][-1], field) for field in PROOF_FIELDS)
        if fault.startswith(("empty:", "drift:")):
            kind, field = fault.split(":")
            value = ((() if field in {"canonical_head_keys", "canonical_head_witness"} else
                      0 if field == "canonical_revision" else "") if kind == "empty"
                     else 99 if field == "canonical_revision" else "foreign")
            run_termination = replace(run_termination, **{field: value})
        elif fault.startswith("key:"):
            keys = list(run_termination.canonical_head_keys)
            keys[int(fault.split(":")[1])] += ":foreign"
            run_termination = replace(run_termination, canonical_head_keys=tuple(keys))
        elif fault == "witness-shape":
            run_termination = replace(run_termination, canonical_head_witness=("invalid",))
        elif fault == "witness-order":
            run_termination = replace(run_termination, canonical_head_witness=tuple(reversed(run_termination.canonical_head_witness)))
        elif fault == "foreign-run":
            run_termination = foreign_result
        elif fault == "run-create":
            from hfa_control.run_create_authority import run_create_operation_id
            probe = await case.store.load_receipt_probe(run, run_create_operation_id(case.req.run_id))
            keys, witness = case.store.authority_head_witness(probe.canonical_store_record, probe.receipt)
            run_termination = replace(run_termination, canonical_head_keys=tuple(keys), canonical_head_witness=tuple(witness))
        elif fault == "stale-head":
            await redis.hset(run_termination.canonical_head_keys[0], mapping=prior)
        elif fault.startswith("stored:"):
            await inject_fault(redis, case.store, run, run_termination.canonical_head_witness, fault.split(":")[1])
        elif fault.startswith("ctx:"):
            ctx = replace(ctx, **{fault.split(":")[1]: "foreign"})
        elif fault.startswith("entry:") or fault == "duplicate-entry-field":
            fields = ["run_id", ctx.run_id, "task_id", ctx.task_id, "tenant_id", ctx.tenant_id]
            if fault == "duplicate-entry-field":
                fields += ["run_id", ctx.run_id]
            else:
                fields[fields.index(fault.split(":")[1]) + 1] = "foreign"
            message_id = await redis.execute_command("XADD", stream, "*", *fields)
            assert await redis.xreadgroup(group, "fault-entry", {stream: ">"}, count=1)
        elif fault == "invalid-message-id":
            message_id = "invalid"
        elif fault == "trimmed-entry":
            await redis.xdel(stream, message_id)
        elif fault == "missing-group":
            await redis.xgroup_destroy(stream, group)
        elif fault in {"missing-stream", "wrong-stream-type"}:
            await redis.delete(stream)
            if fault == "wrong-stream-type":
                await redis.set(stream, "invalid")
        before, writes = await snapshot(redis), await write_counts(redis)
        status = await original_ack(redis, stream, group, message_id, ctx=ctx, run_termination=run_termination)
        assert status == "recovery_ack_proof_conflict"
        assert await snapshot(redis) == before and await write_counts(redis) == writes
        boundaries.append(status)
        return status
    monkeypatch.setattr(runtime, "ack_message", damaged_ack)
    await case.deliver()
    assert boundaries == ["recovery_ack_proof_conflict"]
    if fault not in {"missing-group", "missing-stream", "wrong-stream-type"}:
        ids = await root_redis.xpending_range(case.stream, CONSUMER_GROUP, "-", "+", 10)
        assert case.message_id in [entry["message_id"] for entry in ids]
    assert not seen["execute"] and not seen["claim"] and not seen["task"]


@pytest.mark.parametrize("already_acked", [False, True])
async def test_second_client_invalidates_proof_at_real_ack_lua(alpha, root_redis, monkeypatch, already_acked):
    from redis.asyncio import Redis

    case = await terminal_case(alpha, root_redis)
    if already_acked:
        await case.deliver()
        assert await case.pending() == 0
    seen = observe(case, monkeypatch)
    ready, damaged = asyncio.Event(), asyncio.Event()
    other = Redis.from_url("redis://127.0.0.1:6389/0", decode_responses=True)
    async def lose_receipt():
        await asyncio.wait_for(ready.wait(), 5)
        result = seen["binding"][-1]
        await other.hdel(result.canonical_head_keys[2], result.canonical_head_witness[2])
        damaged.set()
    task = asyncio.create_task(lose_receipt())
    original_run = LuaScriptLoader.run
    checkpoints = []
    async def interleave(loader, **kwargs):
        if loader._path.name != "worker_recovery_ack.lua":
            return await original_run(loader, **kwargs)
        ready.set()
        await asyncio.wait_for(damaged.wait(), 5)
        before, writes = await snapshot(root_redis), await write_counts(root_redis)
        result = await original_run(loader, **kwargs)
        assert result == ["recovery_ack_proof_conflict", "0"]
        assert await snapshot(root_redis) == before and await write_counts(root_redis) == writes
        checkpoints.append(result)
        return result
    monkeypatch.setattr(LuaScriptLoader, "run", interleave)
    try:
        await case.deliver()
        await task
        assert len(checkpoints) == 1
        assert await case.pending() == (0 if already_acked else 1)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await other.aclose()


@pytest.mark.parametrize("fault", ["missing", "wrong-type", "expiring", "schema", "not-ready", "malformed"])
async def test_terminal_event_evidence_remains_required(alpha, root_redis, monkeypatch, fault):
    case = await terminal_case(alpha, root_redis)
    key = RedisKey.run_terminal_event_index()
    if fault in {"missing", "wrong-type"}:
        await root_redis.delete(key)
        if fault == "wrong-type":
            await root_redis.set(key, "wrong")
    elif fault == "expiring":
        await root_redis.pexpire(key, 600000)
    elif fault == "schema":
        await root_redis.hset(key, "__contract__:schema_version", "foreign")
    elif fault == "not-ready":
        await root_redis.hset(key, "__migration__:status", "not-ready")
    else:
        await root_redis.hset(key, "run:" + case.req.run_id, "{")
    seen = observe(case, monkeypatch)
    await case.deliver()
    assert await case.pending() == 1 and not seen["ack"] and not seen["coordinator"]
    assert not seen["execute"] and not seen["claim"] and not seen["task"]
    assert (await case.store.get_aggregate_snapshot(run_terminate_identity(case.req.run_id))).revision == 2
    assert await root_redis.xlen(RedisKey.stream_results()) == 0


@pytest.mark.parametrize("aggregate", ["run", "task"])
@pytest.mark.parametrize("fault", ["receipt_missing", "record_missing", "receipt_checksum", "type_head"])
async def test_invalid_authority_before_finalization_is_read_only(alpha, root_redis, monkeypatch, aggregate, fault):
    case = await terminal_case(alpha, root_redis)
    identity = run_terminate_identity(case.req.run_id) if aggregate == "run" else case.task
    head = await case.store.get_aggregate_snapshot(identity)
    probe = await case.store.load_receipt_probe(identity, head.operation_id)
    _, witness = case.store.authority_head_witness(probe.canonical_store_record, probe.receipt)
    await inject_fault(root_redis, case.store, identity, witness, fault)
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    seen = observe(case, monkeypatch)
    await case.deliver()
    assert await case.pending() == 1 and not seen["ack"] and not seen["binding"]
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == writes
    assert not seen["execute"] and not seen["claim"] and not seen["task"]


@pytest.mark.parametrize("field,value", [
    ("run_id", "foreign"), ("tenant_id", "foreign"), ("worker_instance_id", "foreign"),
    ("scheduler_epoch", "foreign"), ("claim_epoch", "0"), ("claim_epoch", "99"),
])
async def test_task_projection_recovery_rejects_claim_identity_drift(alpha, root_redis, monkeypatch, field, value):
    case = await terminal_case(alpha, root_redis, project_task=False)
    await root_redis.hset(DagRedisKey.task_meta(case.req.root_task_id), field, value)
    seen = observe(case, monkeypatch)
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    await case.deliver()
    assert await case.pending() == 1 and not seen["ack"] and not seen["binding"]
    assert not seen["execute"] and not seen["claim"] and not seen["task"]
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == writes


@pytest.mark.parametrize("missing", ["disabled", "manager", "finalizer"])
async def test_missing_recovery_adapter_never_acks(alpha, root_redis, monkeypatch, missing):
    case = await terminal_case(alpha, root_redis)
    seen = observe(case, monkeypatch)
    if missing == "disabled":
        monkeypatch.setattr(case.worker._run_termination_coordinator, "_enabled", False)
    elif missing == "manager":
        monkeypatch.setattr(case.worker._task_consumer, "_completion_manager", None)
    else:
        monkeypatch.setattr(case.worker._run_termination_coordinator, "finalize_run_from_tasks", None)
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    await case.deliver()
    assert await case.pending() == 1 and not seen["ack"] and not seen["binding"]
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == writes


async def test_canonical_not_ready_blocks_ack_despite_terminal_notification(alpha, root_redis, monkeypatch):
    from hfa.events.codec import deserialize_run_requested
    from hfa_worker.runtime.task_context_builder import build_task_context_from_run_requested

    case = await terminal_case(alpha, root_redis, "running")
    seen = observe(case, monkeypatch)
    ctx = build_task_context_from_run_requested(deserialize_run_requested(case.message),
        worker_id="worker-1", worker_group="group-1", shard=2)
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    await case.worker._consumer._finalize_run_and_ack(
        ctx, terminal_state="done", msg_id=case.message_id, stream=case.stream,
    )
    result = seen["coordinator"][0]
    assert result.status == "not_ready" and not result.finalized and not result.ack_allowed
    assert not result.canonical_head_witness and not result.canonical_head_keys
    assert await case.pending() == 1 and not seen["ack"]
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == writes


async def test_ordinary_first_delivery_keeps_existing_completion_path(alpha, root_redis, monkeypatch):
    case = await terminal_case(alpha, root_redis, "scheduled")
    seen = observe(case, monkeypatch)
    await case.deliver()
    assert await case.pending() == 0
    assert len(seen["execute"]) == len(seen["claim"]) == len(seen["task"]) == 1
    assert not seen["ack"]  # Ordinary completion keeps its existing generic ACK path.
    assert seen["coordinator"][0].canonical_head_witness == seen["binding"][0].canonical_head_witness
    assert (await case.store.get_aggregate_snapshot(case.task)).revision == 4
    assert (await case.store.get_aggregate_snapshot(run_terminate_identity(case.req.run_id))).revision == 2


async def test_exact_task_no_op_after_run_terminal_is_not_delivery_recovery(alpha, root_redis, monkeypatch):
    case = await terminal_case(alpha, root_redis)
    await case.deliver()
    assert await case.pending() == 0
    seen = observe(case, monkeypatch)
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    result = await case.worker._run_termination_coordinator.task_complete(
        task_id=case.req.root_task_id, run_id=case.req.run_id, tenant_id=case.req.tenant_id,
        terminal_state="done", finished_at_ms=4000, reason_code="completed",
        worker_instance_id="worker-1", output_data='{"once":true}',
        expected_scheduler_epoch="epoch-7", expected_claim_epoch=1,
    )
    assert result.completed and result.exact_no_op and not result.ack_allowed
    assert result.run_termination is None
    assert not seen["ack"] and not seen["binding"] and not seen["settle"]
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == writes


async def test_real_legacy_service_result_stays_proofless_and_pending(root_redis, monkeypatch):
    from hfa.events.codec import serialize_event
    from hfa.events.schema import RunRequestedEvent
    from hfa_control.run_terminal_event_evidence import ensure_terminal_event_index
    from tests.integration.test_terminal_task_run_finalization import _seed_run

    await _seed_run(root_redis, run_id="legacy-r4p-run", tenant_id="legacy-r4p-tenant", tasks={"legacy-r4p-task": "done"})
    await ensure_terminal_event_index(root_redis)
    service = WorkerService(root_redis, dict(production=True, worker_id="legacy-r4p-worker",
        worker_group="legacy-group", shards=[2], executor_mode="fake", run_termination_binding_enabled=True))
    assert service._run_termination_coordinator._authority_binding is None
    await service._dag_lua.initialise()
    await service._run_termination_coordinator.initialise()
    stream = RedisKey.stream_shard(2)
    await ensure_consumer_group(root_redis, stream, CONSUMER_GROUP, start_id="0", mkstream=True)
    event = RunRequestedEvent(event_type="TaskRequested", task_id="legacy-r4p-task",
        run_id="legacy-r4p-run", tenant_id="legacy-r4p-tenant", agent_type="default", payload={})
    await root_redis.xadd(stream, serialize_event(event))
    mid, data = (await root_redis.xreadgroup(CONSUMER_GROUP, "previous-worker", {stream: ">"}, count=1))[0][1][0]
    statuses = []
    async def legacy_ack(redis, stream, group, message_id, *, ctx, run_termination):
        assert run_termination.finalized and run_termination.ack_allowed
        assert not run_termination.canonical_head_witness and not run_termination.canonical_head_keys
        before, writes = await snapshot(redis), await write_counts(redis)
        status = await actual_ack(redis, stream, group, message_id, ctx=ctx, run_termination=run_termination)
        assert status == "recovery_ack_proof_conflict"
        assert await snapshot(redis) == before and await write_counts(redis) == writes
        statuses.append(status)
        return status
    monkeypatch.setattr(runtime, "ack_message", legacy_ack)
    await service._consumer._process_message(mid, data, stream, 2)
    assert statuses == ["recovery_ack_proof_conflict"]
    assert (await root_redis.xpending(stream, CONSUMER_GROUP))["pending"] == 1
    assert await root_redis.get(RedisKey.run_state(event.run_id)) == "done"


@pytest.mark.parametrize("failure", ["load", "connection", "malformed-response"])
async def test_ack_infrastructure_failure_never_becomes_success(alpha, root_redis, monkeypatch, failure):
    from redis.exceptions import ConnectionError

    case = await terminal_case(alpha, root_redis)
    seen = observe(case, monkeypatch)
    method = "load" if failure == "load" else "run"
    original = getattr(LuaScriptLoader, method)
    checkpoints = []
    async def fail_at_ack(loader, *args, **kwargs):
        if loader._path.name != "worker_recovery_ack.lua":
            return await original(loader, *args, **kwargs)
        checkpoints.append((await snapshot(root_redis), await write_counts(root_redis)))
        if failure == "malformed-response":
            return ["invalid"]
        raise ConnectionError("controlled ACK infrastructure failure")
    monkeypatch.setattr(LuaScriptLoader, method, fail_at_ack)
    await case.deliver()
    assert len(checkpoints) == 1 and await case.pending() == 1
    assert not any(row[0] in {"acked", "already_acked"} for row in seen["ack"])
    assert await snapshot(root_redis) == checkpoints[0][0]
    assert await write_counts(root_redis) == checkpoints[0][1]
