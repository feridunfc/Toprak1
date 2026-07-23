from __future__ import annotations

import os
from collections import Counter
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import pytest
import pytest_asyncio
import redis.asyncio as redis_asyncio

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.runtime.state_store import StateStore
from hfa_worker.runtime.terminal_duplicate_delivery import (
    TERMINAL_DUPLICATE_DELIVERY,
    classify_terminal_duplicate_delivery,
)
from hfa_worker.task_context import TaskContext

EXPECTED_OPERATIONS = {
    "task_admit",
    "task_dispatch",
    "task_claim",
    "task_heartbeat",
    "task_complete",
    "task_requeue",
    "legacy_run_complete",
    "terminal_duplicate_cleanup",
}

LUA_PATHS = {
    "task_admit": "hfa-core/src/hfa/lua/task_admit.lua",
    "task_dispatch": "hfa-core/src/hfa/lua/task_dispatch_commit.lua",
    "task_claim": "hfa-core/src/hfa/lua/task_claim_start.lua",
    "task_heartbeat": "hfa-core/src/hfa/lua/task_heartbeat.lua",
    "task_complete": "hfa-core/src/hfa/lua/task_complete.lua",
    "task_requeue": "hfa-core/src/hfa/lua/task_requeue.lua",
}


@pytest.fixture(scope="module")
def cardinality_model(
    sprint80_module_loader: Callable[[str], ModuleType],
) -> ModuleType:
    return sprint80_module_loader("transition_cardinality")


def _decode(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


async def _dump_snapshot(redis, keys: list[str]) -> dict[str, bytes | None]:
    return {key: await redis.dump(key) for key in keys}


def _changed_keys(
    before: dict[str, bytes | None],
    after: dict[str, bytes | None],
) -> tuple[str, ...]:
    return tuple(
        sorted(key for key in set(before) | set(after) if before.get(key) != after.get(key))
    )


async def _eval_lua(
    redis,
    *,
    repo_root: Path,
    script_name: str,
    keys: list[str],
    args: list[Any],
):
    source = (repo_root / LUA_PATHS[script_name]).read_text(encoding="utf-8")
    return await redis.eval(source, len(keys), *keys, *[str(value) for value in args])


def _task_keys(task_id: str, tenant_id: str, run_id: str) -> dict[str, str]:
    return {
        "state": f"hfa:dag:task:{task_id}:state",
        "meta": f"hfa:dag:task:{task_id}:meta",
        "remaining": f"hfa:dag:task:{task_id}:remaining_deps",
        "children": f"hfa:dag:task:{task_id}:children",
        "ready_emitted": f"hfa:dag:task:{task_id}:ready_emitted",
        "ready": f"hfa:dag:tenant:{tenant_id}:ready",
        "scheduled": f"hfa:dag:tenant:{tenant_id}:scheduled",
        "running": f"hfa:dag:tenant:{tenant_id}:running",
        "run_tasks": f"hfa:dag:run:{run_id}:tasks",
        "active_tenants": "hfa:dag:tenants:active",
        "output": f"hfa:dag:task:{task_id}:output",
        "reservation": "hfa:dag:worker:s80-card-worker:reservation",
        "reservation_owner": f"hfa:dag:task:{task_id}:reservation_owner",
        "control_stream": "s80:card:stream:control",
        "shard_stream": "s80:card:stream:shard:0",
        "completion_stream": "s80:card:stream:completion",
    }


async def _admit(redis, repo_root: Path, keys: dict[str, str], task_id: str, run_id: str, tenant_id: str):
    return await _eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_admit",
        keys=[
            keys["state"],
            keys["meta"],
            keys["remaining"],
            keys["children"],
            keys["ready_emitted"],
            keys["ready"],
            keys["run_tasks"],
            keys["active_tenants"],
        ],
        args=[
            task_id,
            run_id,
            tenant_id,
            "python",
            5,
            1000,
            '{"prompt":"sprint80 cardinality"}',
            "trace-parent",
            "trace-state",
            0,
            86400,
            86400,
            86400,
            "test",
            "LEAST_LOADED",
        ],
    )


async def _dispatch(redis, repo_root: Path, keys: dict[str, str], task_id: str, run_id: str, tenant_id: str):
    return await _eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_dispatch",
        keys=[
            keys["state"],
            keys["meta"],
            keys["scheduled"],
            keys["control_stream"],
            keys["shard_stream"],
            keys["ready"],
            keys["running"],
        ],
        args=[
            task_id,
            run_id,
            tenant_id,
            "python",
            "s80-group",
            0,
            5,
            1000,
            2000,
            86400,
            86400,
            10000,
            10000,
            "trace-parent",
            "trace-state",
            "LEAST_LOADED",
            "test",
            '{"prompt":"sprint80 cardinality"}',
            "80",
        ],
    )


async def _claim(redis, repo_root: Path, keys: dict[str, str], task_id: str):
    return await _eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_claim",
        keys=[
            keys["state"],
            keys["meta"],
            keys["scheduled"],
            keys["running"],
            keys["reservation"],
            keys["reservation_owner"],
        ],
        args=[task_id, "s80-card-worker", 3000, 86400, 86400, 3000, "", "1"],
    )


async def _heartbeat(redis, repo_root: Path, keys: dict[str, str], task_id: str, tenant_id: str):
    claim_epoch = _decode(await redis.hget(keys["meta"], "claim_epoch"))
    return await _eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_heartbeat",
        keys=[keys["state"], keys["meta"], keys["running"]],
        args=[task_id, tenant_id, "s80-card-worker", claim_epoch, 4000],
    )


async def _complete(redis, repo_root: Path, keys: dict[str, str], task_id: str, run_id: str, tenant_id: str):
    claim_epoch = _decode(await redis.hget(keys["meta"], "claim_epoch"))
    return await _eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_complete",
        keys=[
            keys["state"],
            keys["meta"],
            keys["children"],
            keys["output"],
            keys["ready"],
            keys["running"],
        ],
        args=[
            task_id,
            run_id,
            tenant_id,
            "done",
            5000,
            86400,
            86400,
            86400,
            5000,
            "SUCCESS",
            "s80-card-worker",
            '{"result":"ok"}',
            "hfa:dag:task:",
            ":state",
            "hfa:dag:task:",
            ":remaining_deps",
            "hfa:dag:task:",
            ":ready_emitted",
            "",
            claim_epoch,
        ],
    )


async def _stream_records(redis, key: str, start_length: int, source: str, model: ModuleType):
    rows = await redis.xrange(key, min="-", max="+")
    records = []
    for _, raw_fields in rows[start_length:]:
        fields = model.decode_mapping(raw_fields)
        records.append(model.classify_record(source=source, source_key=key, fields=fields))
    return records


async def _observe_lua_operation(
    redis,
    *,
    model: ModuleType,
    operation: str,
    state_key: str,
    all_keys: list[str],
    coordination_keys: set[str],
    projection_keys: set[str],
    record_sources: list[tuple[str, str]],
    action,
    notes: tuple[str, ...] = (),
):
    before_dump = await _dump_snapshot(redis, all_keys)
    before_state = _decode(await redis.get(state_key))
    stream_lengths = {key: await redis.xlen(key) for key, _ in record_sources}

    await action()

    after_dump = await _dump_snapshot(redis, all_keys)
    after_state = _decode(await redis.get(state_key))
    changed = _changed_keys(before_dump, after_dump)
    records = []
    for key, source in record_sources:
        records.extend(await _stream_records(redis, key, stream_lengths[key], source, model))
    counts = Counter(record.primary_class for record in records)

    return model.OperationObservation(
        operation=operation,
        transaction_boundary="single_redis_lua_call",
        state_before=before_state,
        state_after=after_state,
        aggregate_revision_candidate=0,
        execution_lifecycle_mutation=int(before_state != after_state),
        coordination_mutation=sum(key in coordination_keys for key in changed),
        projection_mutation=sum(key in projection_keys for key in changed),
        transport_append=counts["TransportMessage"],
        transport_ack=0,
        audit_append=counts["AuditEvent"],
        canonical_transition_record_count=counts["CanonicalTransitionRecord"],
        changed_keys=changed,
        records=tuple(records),
        notes=notes,
    )


async def _observe_legacy_run_complete(redis, model: ModuleType):
    run_id = "s80-card-legacy-run"
    state_key = RedisKey.run_state(run_id)
    meta_key = f"hfa:run:meta:{run_id}"
    result_key = f"hfa:run:result:{run_id}"
    claim_key = f"hfa:run:claim:{run_id}"
    running_key = "hfa:cp:running"
    result_stream = RedisKey.stream_results()
    keys = [state_key, meta_key, result_key, claim_key, running_key, result_stream]

    await redis.set(state_key, "running", ex=86400)
    await redis.set(claim_key, "s80-card-worker", ex=60)
    await redis.zadd(running_key, {run_id: 1})
    before_dump = await _dump_snapshot(redis, keys)
    before_state = _decode(await redis.get(state_key))
    before_stream = await redis.xlen(result_stream)
    store = StateStore(redis)
    lifecycle_changes = 0

    await store.store_result(run_id, "s80-tenant", "done", {"result": "ok"}, 0, 0)
    state_pre_first = _decode(await redis.get(state_key))
    await store.transition_state(run_id, "done")
    state_post_first = _decode(await redis.get(state_key))
    lifecycle_changes += int(state_pre_first != state_post_first)

    await redis.xadd(
        result_stream,
        {
            "event_type": "RunCompleted",
            "run_id": run_id,
            "tenant_id": "s80-tenant",
            "worker_id": "s80-card-worker",
        },
    )

    state_pre_second = _decode(await redis.get(state_key))
    await store.mark_completed(run_id)
    state_post_second = _decode(await redis.get(state_key))
    lifecycle_changes += int(state_pre_second != state_post_second)

    after_dump = await _dump_snapshot(redis, keys)
    changed = _changed_keys(before_dump, after_dump)
    records = await _stream_records(redis, result_stream, before_stream, "result_stream", model)
    counts = Counter(record.primary_class for record in records)

    return model.OperationObservation(
        operation="legacy_run_complete",
        transaction_boundary="multi_command_compatibility_path",
        state_before=before_state,
        state_after=_decode(await redis.get(state_key)),
        aggregate_revision_candidate=0,
        execution_lifecycle_mutation=lifecycle_changes,
        coordination_mutation=sum(key in {claim_key, running_key} for key in changed),
        projection_mutation=sum(key in {meta_key, result_key} for key in changed),
        transport_append=counts["TransportMessage"],
        transport_ack=0,
        audit_append=counts["AuditEvent"],
        canonical_transition_record_count=counts["CanonicalTransitionRecord"],
        changed_keys=changed,
        records=tuple(records),
        notes=(
            "legacy path attempts terminal transition twice",
            "only one observed state value change is committed",
        ),
    )


async def _observe_terminal_duplicate_cleanup(redis, model: ModuleType):
    task_id = "s80-card-terminal-duplicate"
    run_id = "s80-card-terminal-duplicate-run"
    state_key = DagRedisKey.task_state(task_id)
    meta_key = DagRedisKey.task_meta(task_id)
    stream = "s80:card:terminal-duplicate:stream"
    group = "s80-card-group"
    consumer = "s80-card-consumer"

    await redis.set(state_key, "done")
    await redis.hset(meta_key, mapping={"task_id": task_id, "run_id": run_id})
    await redis.xgroup_create(stream, group, id="0", mkstream=True)
    message_id = await redis.xadd(
        stream,
        {"event_type": "TaskRequested", "task_id": task_id, "run_id": run_id},
    )
    await redis.xreadgroup(group, consumer, {stream: ">"}, count=1)
    pending_before = await redis.xpending_range(stream, group, min="-", max="+", count=10)
    state_before = _decode(await redis.get(state_key))
    stream_before = await redis.xlen(stream)

    ctx = TaskContext(
        task_id=task_id,
        run_id=run_id,
        tenant_id="s80-tenant",
        agent_type="python",
        worker_group="s80-group",
        worker_instance_id="s80-card-worker",
        payload={},
    )
    decision = await classify_terminal_duplicate_delivery(
        redis,
        ctx,
        message_task_id=task_id,
        message_run_id=run_id,
    )
    assert decision.status == TERMINAL_DUPLICATE_DELIVERY
    assert decision.ack_allowed is True
    acked = int(await redis.xack(stream, group, message_id))
    pending_after = await redis.xpending_range(stream, group, min="-", max="+", count=10)
    records = await _stream_records(redis, stream, stream_before, "shard_stream", model)

    return model.OperationObservation(
        operation="terminal_duplicate_cleanup",
        transaction_boundary="read_classification_plus_transport_ack",
        state_before=state_before,
        state_after=_decode(await redis.get(state_key)),
        aggregate_revision_candidate=0,
        execution_lifecycle_mutation=0,
        coordination_mutation=int(len(pending_before) != len(pending_after)),
        projection_mutation=0,
        transport_append=0,
        transport_ack=acked,
        audit_append=0,
        canonical_transition_record_count=0,
        changed_keys=(),
        records=tuple(records),
        notes=(
            f"ack_policy={decision.ack_policy}",
            "terminal state and metadata are read-only during classification",
        ),
    )


async def _build_report(redis, repo_root: Path, model: ModuleType) -> dict:
    await redis.flushdb()
    observations = []
    task_id = "s80-card-main"
    run_id = "s80-card-main-run"
    tenant_id = "s80-card-main-tenant"
    keys = _task_keys(task_id, tenant_id, run_id)

    observations.append(
        await _observe_lua_operation(
            redis,
            model=model,
            operation="task_admit",
            state_key=keys["state"],
            all_keys=[
                keys["state"], keys["meta"], keys["remaining"], keys["children"],
                keys["ready_emitted"], keys["ready"], keys["run_tasks"], keys["active_tenants"],
            ],
            coordination_keys={keys["remaining"], keys["ready"], keys["active_tenants"]},
            projection_keys={keys["meta"], keys["children"], keys["ready_emitted"], keys["run_tasks"]},
            record_sources=[],
            action=lambda: _admit(redis, repo_root, keys, task_id, run_id, tenant_id),
        )
    )
    observations.append(
        await _observe_lua_operation(
            redis,
            model=model,
            operation="task_dispatch",
            state_key=keys["state"],
            all_keys=[keys["state"], keys["meta"], keys["ready"], keys["scheduled"], keys["control_stream"], keys["shard_stream"]],
            coordination_keys={keys["ready"], keys["scheduled"]},
            projection_keys={keys["meta"]},
            record_sources=[(keys["control_stream"], "control_stream"), (keys["shard_stream"], "shard_stream")],
            action=lambda: _dispatch(redis, repo_root, keys, task_id, run_id, tenant_id),
        )
    )
    observations.append(
        await _observe_lua_operation(
            redis,
            model=model,
            operation="task_claim",
            state_key=keys["state"],
            all_keys=[keys["state"], keys["meta"], keys["scheduled"], keys["running"], keys["reservation"], keys["reservation_owner"]],
            coordination_keys={keys["scheduled"], keys["running"], keys["reservation"], keys["reservation_owner"]},
            projection_keys={keys["meta"]},
            record_sources=[],
            action=lambda: _claim(redis, repo_root, keys, task_id),
        )
    )
    observations.append(
        await _observe_lua_operation(
            redis,
            model=model,
            operation="task_heartbeat",
            state_key=keys["state"],
            all_keys=[keys["state"], keys["meta"], keys["running"]],
            coordination_keys={keys["running"]},
            projection_keys={keys["meta"]},
            record_sources=[],
            action=lambda: _heartbeat(redis, repo_root, keys, task_id, tenant_id),
            notes=("heartbeat is coordination/liveness, not a lifecycle transition",),
        )
    )
    observations.append(
        await _observe_lua_operation(
            redis,
            model=model,
            operation="task_complete",
            state_key=keys["state"],
            all_keys=[keys["state"], keys["meta"], keys["output"], keys["running"], keys["ready"], keys["children"]],
            coordination_keys={keys["running"], keys["ready"]},
            projection_keys={keys["meta"], keys["output"]},
            record_sources=[],
            action=lambda: _complete(redis, repo_root, keys, task_id, run_id, tenant_id),
        )
    )

    requeue_task = "s80-card-requeue"
    requeue_run = "s80-card-requeue-run"
    requeue_tenant = "s80-card-requeue-tenant"
    requeue_keys = _task_keys(requeue_task, requeue_tenant, requeue_run)
    await _admit(redis, repo_root, requeue_keys, requeue_task, requeue_run, requeue_tenant)
    await _dispatch(redis, repo_root, requeue_keys, requeue_task, requeue_run, requeue_tenant)
    await _claim(redis, repo_root, requeue_keys, requeue_task)
    observations.append(
        await _observe_lua_operation(
            redis,
            model=model,
            operation="task_requeue",
            state_key=requeue_keys["state"],
            all_keys=[requeue_keys["state"], requeue_keys["meta"], requeue_keys["ready"], requeue_keys["running"], requeue_keys["completion_stream"]],
            coordination_keys={requeue_keys["ready"], requeue_keys["running"]},
            projection_keys={requeue_keys["meta"]},
            record_sources=[(requeue_keys["completion_stream"], "completion_stream")],
            action=lambda: _eval_lua(
                redis,
                repo_root=repo_root,
                script_name="task_requeue",
                keys=[requeue_keys["state"], requeue_keys["meta"], requeue_keys["ready"], requeue_keys["running"], requeue_keys["completion_stream"]],
                args=[requeue_task, requeue_tenant, "running", 6000, 6000, 3, "STALE_HEARTBEAT", 10000],
            ),
        )
    )
    observations.append(await _observe_legacy_run_complete(redis, model))
    observations.append(await _observe_terminal_duplicate_cleanup(redis, model))
    return model.render_report(observations)


@pytest_asyncio.fixture(scope="module")
async def cardinality_report(repo_root: Path, cardinality_model: ModuleType):
    url = os.getenv("SPRINT80_REDIS_URL", "")
    if not url:
        pytest.skip("SPRINT80_REDIS_URL is required for cardinality diagnostics")
    client = redis_asyncio.Redis.from_url(url, decode_responses=False)
    await client.ping()
    try:
        report = await _build_report(client, repo_root, cardinality_model)
        cardinality_model.write_json(
            repo_root / "local_out/sprint80/transition_cardinality.json",
            report,
        )
        yield report
    finally:
        await client.flushdb()
        await client.aclose()


def _operation(report: dict, name: str) -> dict:
    return next(row for row in report["operations"] if row["operation"] == name)


@pytest.mark.sprint80_reality
def test_exact_cardinality_operations_are_observed(cardinality_report: dict):
    assert cardinality_report["operation_count"] == 8
    assert {row["operation"] for row in cardinality_report["operations"]} == EXPECTED_OPERATIONS


@pytest.mark.sprint80_reality
def test_heartbeat_is_coordination_not_lifecycle(cardinality_report: dict):
    heartbeat = _operation(cardinality_report, "task_heartbeat")
    assert heartbeat["state_before"] == "running"
    assert heartbeat["state_after"] == "running"
    assert heartbeat["execution_lifecycle_mutation"] == 0
    assert heartbeat["coordination_mutation"] >= 1
    assert heartbeat["records"] == []


@pytest.mark.sprint80_reality
def test_dispatch_emits_transport_and_runtime_notification(cardinality_report: dict):
    dispatch = _operation(cardinality_report, "task_dispatch")
    classes = Counter(record["primary_class"] for record in dispatch["records"])
    assert dispatch["state_before"] == "ready"
    assert dispatch["state_after"] == "scheduled"
    assert classes == Counter({"RuntimeTransitionNotification": 1, "TransportMessage": 1})


@pytest.mark.sprint80_reality
def test_requeue_and_legacy_completion_record_taxonomy(cardinality_report: dict):
    requeue = _operation(cardinality_report, "task_requeue")
    legacy = _operation(cardinality_report, "legacy_run_complete")
    assert [record["primary_class"] for record in requeue["records"]] == [
        "RuntimeTransitionNotification"
    ]
    assert [record["primary_class"] for record in legacy["records"]] == [
        "EffectResultMessage"
    ]


@pytest.mark.sprint80_reality
def test_terminal_duplicate_cleanup_only_acks_transport(cardinality_report: dict):
    cleanup = _operation(cardinality_report, "terminal_duplicate_cleanup")
    assert cleanup["state_before"] == "done"
    assert cleanup["state_after"] == "done"
    assert cleanup["execution_lifecycle_mutation"] == 0
    assert cleanup["transport_ack"] == 1
    assert cleanup["records"] == []


@pytest.mark.sprint80_reality
def test_every_observed_record_has_one_primary_taxonomy_class(cardinality_report: dict):
    for operation in cardinality_report["operations"]:
        for record in operation["records"]:
            assert record["primary_class"] in cardinality_model_from_report_classes()
            assert isinstance(record["primary_class"], str)
            assert record["primary_class"]


def cardinality_model_from_report_classes() -> set[str]:
    return {
        "TransportMessage",
        "RuntimeTransitionNotification",
        "EffectResultMessage",
        "AuditEvent",
        "EvidenceArtifact",
        "CanonicalTransitionRecord",
        "Unknown",
    }


@pytest.mark.sprint80_reality
def test_no_observed_record_matches_canonical_transition_record_schema(cardinality_report: dict):
    assert cardinality_report["canonical_transition_record_count"] == 0
    assert cardinality_report["canonical_schema_matches"] == []


@pytest.mark.sprint80_reality
def test_no_operation_exposes_aggregate_revision_evidence(cardinality_report: dict):
    assert all(
        operation["aggregate_revision_candidate"] == 0
        for operation in cardinality_report["operations"]
    )


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Observed lifecycle mutations do not produce exactly one CanonicalTransitionRecord; "
        "current records are transport, runtime notifications, or effect results"
    ),
)
def test_each_lifecycle_mutation_produces_exactly_one_canonical_transition_record(
    cardinality_report: dict,
):
    lifecycle_operations = [
        operation
        for operation in cardinality_report["operations"]
        if operation["execution_lifecycle_mutation"] > 0
    ]
    assert lifecycle_operations
    assert all(
        operation["canonical_transition_record_count"] == 1
        for operation in lifecycle_operations
    )
