from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import pytest
import redis.asyncio as redis_asyncio

import hfa_control.shard as shard_module
from hfa.runtime.state_store import StateStore
from hfa_control.event_store import EventStore
from hfa_control.shard import ShardOwnershipManager
from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
    TerminalDuplicateCleanupAuditEvent,
    append_terminal_duplicate_cleanup_audit_event,
)
from hfa_worker.heartbeat import HEARTBEAT_STREAM, WorkerHeartbeatPublisher

MAIN_TTL_SECONDS = 3
COORDINATION_TTL_SECONDS = 2

EXPECTED_STATE_FAMILIES = {
    "run_state",
    "run_meta",
    "run_result",
    "run_claim",
    "dag_task_state",
    "dag_task_meta",
    "task_output",
    "remaining_dependencies",
    "ready_emitted",
    "ready_queue",
    "scheduled_zset",
    "running_zset",
    "worker_reservation",
    "reservation_owner",
    "shard_lease",
    "shard_owner_projection",
    "event_store_list",
    "control_stream",
    "shard_stream",
    "completion_stream",
    "worker_heartbeat_stream",
    "operator_audit_stream",
}


def _load_base_cardinality_module(repo_root: Path) -> ModuleType:
    path = repo_root / "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py"
    name = "sprint80_ttl_base_cardinality"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load cardinality helpers: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _decode(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


async def _pttl(redis, key: str) -> int:
    return int(await redis.pttl(key))


async def _eval_file(redis, repo_root: Path, relative_path: str, keys: list[str], args: list[Any]):
    source = (repo_root / relative_path).read_text(encoding="utf-8")
    return await redis.eval(source, len(keys), *keys, *[str(value) for value in args])


async def _admit_with_ttl(
    redis,
    base: ModuleType,
    repo_root: Path,
    keys: dict[str, str],
    task_id: str,
    run_id: str,
    tenant_id: str,
    ttl: int,
):
    return await base._eval_lua(
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
            '{"prompt":"sprint80 ttl"}',
            "trace-parent",
            "trace-state",
            0,
            ttl,
            ttl,
            ttl,
            "test",
            "LEAST_LOADED",
        ],
    )


async def _dispatch_with_ttl(
    redis,
    base: ModuleType,
    repo_root: Path,
    keys: dict[str, str],
    task_id: str,
    run_id: str,
    tenant_id: str,
    ttl: int,
    scheduler_epoch: str,
):
    return await base._eval_lua(
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
            ttl,
            ttl,
            10000,
            10000,
            "trace-parent",
            "trace-state",
            "LEAST_LOADED",
            "test",
            '{"prompt":"sprint80 ttl"}',
            scheduler_epoch,
        ],
    )


async def _reserve(
    redis,
    repo_root: Path,
    keys: dict[str, str],
    task_id: str,
    *,
    worker_id: str,
    scheduler_epoch: str,
    ttl: int,
):
    return await _eval_file(
        redis,
        repo_root,
        "hfa-core/src/hfa/lua/reserve_worker.lua",
        [keys["reservation"], keys["reservation_owner"]],
        [worker_id, task_id, scheduler_epoch, 2500, ttl, "s80-scheduler"],
    )


async def _claim_with_ttl(
    redis,
    base: ModuleType,
    repo_root: Path,
    keys: dict[str, str],
    task_id: str,
    *,
    worker_id: str,
    scheduler_epoch: str,
    ttl: int,
):
    return await base._eval_lua(
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
        args=[task_id, worker_id, 3000, ttl, ttl, 3000, scheduler_epoch, "0"],
    )


async def _heartbeat(
    redis,
    base: ModuleType,
    repo_root: Path,
    keys: dict[str, str],
    task_id: str,
    tenant_id: str,
    worker_id: str,
):
    claim_epoch = _decode(await redis.hget(keys["meta"], "claim_epoch"))
    return await base._eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_heartbeat",
        keys=[keys["state"], keys["meta"], keys["running"]],
        args=[task_id, tenant_id, worker_id, claim_epoch, 4000],
    )


async def _complete_with_ttl(
    redis,
    base: ModuleType,
    repo_root: Path,
    keys: dict[str, str],
    task_id: str,
    run_id: str,
    tenant_id: str,
    *,
    worker_id: str,
    scheduler_epoch: str,
    ttl: int,
):
    claim_epoch = _decode(await redis.hget(keys["meta"], "claim_epoch"))
    return await base._eval_lua(
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
            ttl,
            ttl,
            ttl,
            5000,
            "SUCCESS",
            worker_id,
            '{"result":"ok"}',
            "hfa:dag:task:",
            ":state",
            "hfa:dag:task:",
            ":remaining_deps",
            "hfa:dag:task:",
            ":ready_emitted",
            scheduler_epoch,
            claim_epoch,
        ],
    )


async def _requeue(
    redis,
    base: ModuleType,
    repo_root: Path,
    keys: dict[str, str],
    task_id: str,
    tenant_id: str,
):
    return await base._eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_requeue",
        keys=[
            keys["state"],
            keys["meta"],
            keys["ready"],
            keys["running"],
            keys["completion_stream"],
        ],
        args=[task_id, tenant_id, "running", 6000, 6000, 3, "STALE_HEARTBEAT", 10000],
    )


def _append_pttl(store: dict[str, dict[str, int]], family: str, stage: str, value: int) -> None:
    store.setdefault(family, {})[stage] = int(value)


async def _record_task_pttls(
    redis,
    store: dict[str, dict[str, int]],
    stage: str,
    keys: dict[str, str],
) -> None:
    mapping = {
        "dag_task_state": keys["state"],
        "dag_task_meta": keys["meta"],
        "task_output": keys["output"],
        "remaining_dependencies": keys["remaining"],
        "ready_emitted": keys["ready_emitted"],
        "ready_queue": keys["ready"],
        "scheduled_zset": keys["scheduled"],
        "running_zset": keys["running"],
        "control_stream": keys["control_stream"],
        "shard_stream": keys["shard_stream"],
        "completion_stream": keys["completion_stream"],
        "worker_reservation": keys["reservation"],
        "reservation_owner": keys["reservation_owner"],
    }
    for family, key in mapping.items():
        _append_pttl(store, family, stage, await _pttl(redis, key))


async def _build_report(
    *,
    redis_url: str,
    repo_root: Path,
    model: ModuleType,
) -> dict[str, Any]:
    redis = redis_asyncio.Redis.from_url(redis_url, decode_responses=False)
    await redis.ping()
    await redis.flushdb()
    base = _load_base_cardinality_module(repo_root)
    pttls: dict[str, dict[str, int]] = {}
    lifecycle: dict[str, Any] = {}
    old_run_meta_ttl = StateStore.RUN_META_TTL_SECONDS
    old_run_state_ttl = StateStore.RUN_STATE_TTL_SECONDS
    old_claim_ttl = StateStore.CLAIM_TTL
    old_owner_ttl = shard_module.OWNER_TTL

    try:
        task_id = "s80-ttl-main"
        run_id = "s80-ttl-main-run"
        tenant_id = "s80-ttl-main-tenant"
        worker_id = "s80-ttl-worker"
        scheduler_epoch = "s80-ttl-epoch"
        keys = base._task_keys(task_id, tenant_id, run_id)

        await _record_task_pttls(redis, pttls, "initial", keys)
        await _admit_with_ttl(
            redis, base, repo_root, keys, task_id, run_id, tenant_id, MAIN_TTL_SECONDS
        )
        lifecycle["after_admit_state"] = _decode(await redis.get(keys["state"]))
        await _record_task_pttls(redis, pttls, "after_admit", keys)

        await _dispatch_with_ttl(
            redis,
            base,
            repo_root,
            keys,
            task_id,
            run_id,
            tenant_id,
            MAIN_TTL_SECONDS,
            scheduler_epoch,
        )
        lifecycle["after_dispatch_state"] = _decode(await redis.get(keys["state"]))
        await _record_task_pttls(redis, pttls, "after_dispatch", keys)

        await _reserve(
            redis,
            repo_root,
            keys,
            task_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            ttl=COORDINATION_TTL_SECONDS,
        )
        await _record_task_pttls(redis, pttls, "after_reservation", keys)

        await _claim_with_ttl(
            redis,
            base,
            repo_root,
            keys,
            task_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            ttl=MAIN_TTL_SECONDS,
        )
        lifecycle["after_claim_state"] = _decode(await redis.get(keys["state"]))
        await _record_task_pttls(redis, pttls, "after_claim", keys)

        await asyncio.sleep(0.7)
        await _record_task_pttls(redis, pttls, "before_heartbeat", keys)
        await _heartbeat(redis, base, repo_root, keys, task_id, tenant_id, worker_id)
        lifecycle["after_heartbeat_state"] = _decode(await redis.get(keys["state"]))
        await _record_task_pttls(redis, pttls, "after_heartbeat", keys)

        await _complete_with_ttl(
            redis,
            base,
            repo_root,
            keys,
            task_id,
            run_id,
            tenant_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            ttl=MAIN_TTL_SECONDS,
        )
        lifecycle["after_complete_state"] = _decode(await redis.get(keys["state"]))
        await _record_task_pttls(redis, pttls, "after_complete", keys)

        event_store = EventStore(redis)
        await event_store.append_event(
            run_id,
            EventStore.EVENT_TASK_COMPLETED,
            worker_id=worker_id,
            details={"task_id": task_id, "state": "done"},
        )
        event_key = EventStore.event_key(run_id)
        _append_pttl(pttls, "event_store_list", "after_append", await _pttl(redis, event_key))

        StateStore.RUN_META_TTL_SECONDS = COORDINATION_TTL_SECONDS
        StateStore.RUN_STATE_TTL_SECONDS = COORDINATION_TTL_SECONDS
        StateStore.CLAIM_TTL = COORDINATION_TTL_SECONDS
        run_store = StateStore(redis)
        legacy_run = "s80-ttl-legacy-run"
        await run_store.create_run_meta(legacy_run, {"state": "queued", "tenant_id": "s80"})
        await run_store.transition_state(legacy_run, "scheduled")
        await run_store.mark_running(legacy_run, "s80-legacy-worker", "s80-group", 0)
        _append_pttl(pttls, "run_state", "after_running", await _pttl(redis, run_store._run_state_key(legacy_run)))
        _append_pttl(pttls, "run_meta", "after_running", await _pttl(redis, run_store._run_meta_key(legacy_run)))
        _append_pttl(pttls, "run_claim", "after_running", await _pttl(redis, run_store._claim_key(legacy_run)))
        await run_store.store_result(legacy_run, "s80", "done", {"ok": True}, 0, 0)
        _append_pttl(pttls, "run_result", "after_store_result", await _pttl(redis, run_store._run_result_key(legacy_run)))
        await run_store.mark_completed(legacy_run)
        _append_pttl(pttls, "run_state", "after_complete", await _pttl(redis, run_store._run_state_key(legacy_run)))
        _append_pttl(pttls, "run_meta", "after_complete", await _pttl(redis, run_store._run_meta_key(legacy_run)))
        _append_pttl(pttls, "run_claim", "after_complete", await _pttl(redis, run_store._claim_key(legacy_run)))
        _append_pttl(pttls, "run_result", "after_complete", await _pttl(redis, run_store._run_result_key(legacy_run)))

        reservation_task = "s80-ttl-reservation-only"
        reservation_keys = base._task_keys(
            reservation_task, "s80-reservation-tenant", "s80-reservation-run"
        )
        reserve_result = await _reserve(
            redis,
            repo_root,
            reservation_keys,
            reservation_task,
            worker_id="s80-reservation-worker",
            scheduler_epoch="s80-reservation-epoch",
            ttl=COORDINATION_TTL_SECONDS,
        )
        lifecycle["reservation_created"] = _decode(reserve_result[0])
        _append_pttl(
            pttls,
            "worker_reservation",
            "standalone_created",
            await _pttl(redis, reservation_keys["reservation"]),
        )
        _append_pttl(
            pttls,
            "reservation_owner",
            "standalone_created",
            await _pttl(redis, reservation_keys["reservation_owner"]),
        )

        shard_module.OWNER_TTL = COORDINATION_TTL_SECONDS
        shard_manager = ShardOwnershipManager(redis, object())
        lifecycle["shard_claimed"] = await shard_manager.claim_shard(7, "s80-shard-group")
        shard_key = shard_module.RedisKey.cp_shard_owner(7)
        shard_projection = shard_module.RedisKey.cp_shard_owners()
        _append_pttl(pttls, "shard_lease", "after_claim", await _pttl(redis, shard_key))
        _append_pttl(
            pttls,
            "shard_owner_projection",
            "after_claim",
            await _pttl(redis, shard_projection),
        )
        await asyncio.sleep(0.4)
        shard_before_renew = await _pttl(redis, shard_key)
        lifecycle["shard_renewed"] = await shard_manager.renew_shard(7, "s80-shard-group")
        shard_after_renew = await _pttl(redis, shard_key)
        _append_pttl(pttls, "shard_lease", "before_renew", shard_before_renew)
        _append_pttl(pttls, "shard_lease", "after_renew", shard_after_renew)

        heartbeat = WorkerHeartbeatPublisher(
            redis,
            worker_id="s80-heartbeat-worker",
            worker_group="s80-group",
            region="test",
            shards=[7],
            capacity=1,
            inflight_fn=lambda: 0,
            is_draining_fn=lambda: False,
            version="s80",
            capabilities=["python"],
        )
        await heartbeat.publish_now()
        _append_pttl(
            pttls,
            "worker_heartbeat_stream",
            "after_publish",
            await _pttl(redis, HEARTBEAT_STREAM),
        )

        for phase in (AUDIT_PHASE_INTENT, AUDIT_PHASE_OUTCOME):
            result = await append_terminal_duplicate_cleanup_audit_event(
                redis,
                TerminalDuplicateCleanupAuditEvent(
                    command_attempt_id="s80-ttl-audit",
                    event_phase=phase,
                    task_id="s80-ttl-audit-task",
                    status="observed",
                ),
            )
            assert result.written is True
        _append_pttl(
            pttls,
            "operator_audit_stream",
            "after_append",
            await _pttl(redis, TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM),
        )

        requeue_task = "s80-ttl-requeue"
        requeue_run = "s80-ttl-requeue-run"
        requeue_tenant = "s80-ttl-requeue-tenant"
        requeue_keys = base._task_keys(requeue_task, requeue_tenant, requeue_run)
        await _admit_with_ttl(
            redis,
            base,
            repo_root,
            requeue_keys,
            requeue_task,
            requeue_run,
            requeue_tenant,
            MAIN_TTL_SECONDS,
        )
        await _dispatch_with_ttl(
            redis,
            base,
            repo_root,
            requeue_keys,
            requeue_task,
            requeue_run,
            requeue_tenant,
            MAIN_TTL_SECONDS,
            "s80-requeue-epoch",
        )
        await _reserve(
            redis,
            repo_root,
            requeue_keys,
            requeue_task,
            worker_id="s80-requeue-worker",
            scheduler_epoch="s80-requeue-epoch",
            ttl=COORDINATION_TTL_SECONDS,
        )
        await _claim_with_ttl(
            redis,
            base,
            repo_root,
            requeue_keys,
            requeue_task,
            worker_id="s80-requeue-worker",
            scheduler_epoch="s80-requeue-epoch",
            ttl=MAIN_TTL_SECONDS,
        )
        _append_pttl(pttls, "dag_task_state", "requeue_before", await _pttl(redis, requeue_keys["state"]))
        _append_pttl(pttls, "dag_task_meta", "requeue_before", await _pttl(redis, requeue_keys["meta"]))
        _append_pttl(pttls, "ready_queue", "requeue_before", await _pttl(redis, requeue_keys["ready"]))
        await _requeue(redis, base, repo_root, requeue_keys, requeue_task, requeue_tenant)
        lifecycle["after_requeue_state"] = _decode(await redis.get(requeue_keys["state"]))
        _append_pttl(pttls, "dag_task_state", "requeue_after", await _pttl(redis, requeue_keys["state"]))
        _append_pttl(pttls, "dag_task_meta", "requeue_after", await _pttl(redis, requeue_keys["meta"]))
        _append_pttl(pttls, "ready_queue", "requeue_after", await _pttl(redis, requeue_keys["ready"]))
        _append_pttl(
            pttls,
            "completion_stream",
            "requeue_after",
            await _pttl(redis, requeue_keys["completion_stream"]),
        )

        await asyncio.sleep(MAIN_TTL_SECONDS + 0.5)
        await _record_task_pttls(redis, pttls, "after_accelerated_expiry", keys)
        _append_pttl(
            pttls,
            "event_store_list",
            "after_accelerated_expiry",
            await _pttl(redis, event_key),
        )
        _append_pttl(
            pttls,
            "run_state",
            "after_accelerated_expiry",
            await _pttl(redis, run_store._run_state_key(legacy_run)),
        )
        _append_pttl(
            pttls,
            "run_meta",
            "after_accelerated_expiry",
            await _pttl(redis, run_store._run_meta_key(legacy_run)),
        )
        _append_pttl(
            pttls,
            "run_result",
            "after_accelerated_expiry",
            await _pttl(redis, run_store._run_result_key(legacy_run)),
        )
        _append_pttl(
            pttls,
            "worker_reservation",
            "after_accelerated_expiry",
            await _pttl(redis, reservation_keys["reservation"]),
        )
        _append_pttl(
            pttls,
            "reservation_owner",
            "after_accelerated_expiry",
            await _pttl(redis, reservation_keys["reservation_owner"]),
        )
        _append_pttl(pttls, "shard_lease", "after_accelerated_expiry", await _pttl(redis, shard_key))
        _append_pttl(
            pttls,
            "shard_owner_projection",
            "before_orphan_cleanup",
            await _pttl(redis, shard_projection),
        )
        await shard_manager._check_orphans()
        lifecycle["shard_projection_after_cleanup"] = _decode(
            await redis.hget(shard_projection, 7)
        )
        lifecycle["reservation_recreated_after_expiry"] = _decode(
            (
                await _reserve(
                    redis,
                    repo_root,
                    reservation_keys,
                    reservation_task,
                    worker_id="s80-reservation-worker",
                    scheduler_epoch="s80-reservation-epoch-2",
                    ttl=COORDINATION_TTL_SECONDS,
                )
            )[0]
        )
        lifecycle["shard_reclaimed_after_expiry"] = await shard_manager.claim_shard(
            7, "s80-shard-group"
        )

        observations = [
            model.make_observation(
                state_family="run_state",
                key_pattern="hfa:run:state:<run_id>",
                authority_class="canonical_candidate",
                durability_class="durable_truth_candidate",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["run_state"],
                ttl_refreshed_by=("transition_state", "mark_running", "mark_completed"),
                expiry_observed=pttls["run_state"]["after_accelerated_expiry"] == -2,
                recovery_source="unknown",
                reconstructable=False,
                reconstruction_tested=True,
                notes=("compatibility run state expires by default",),
            ),
            model.make_observation(
                state_family="run_meta",
                key_pattern="hfa:run:meta:<run_id>",
                authority_class="compatibility",
                durability_class="reconstructable_projection",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["run_meta"],
                ttl_refreshed_by=("create_run_meta",),
                expiry_observed=pttls["run_meta"]["after_accelerated_expiry"] == -2,
                recovery_source="run_state_and_result_partial",
                reconstructable=None,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="run_result",
                key_pattern="hfa:run:result:<run_id>",
                authority_class="compatibility",
                durability_class="retained_execution_truth",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["run_result"],
                ttl_refreshed_by=("store_result",),
                expiry_observed=pttls["run_result"]["after_accelerated_expiry"] == -2,
                recovery_source="result_stream_partial",
                reconstructable=False,
                reconstruction_tested=True,
            ),
            model.make_observation(
                state_family="run_claim",
                key_pattern="hfa:run:claim:<run_id>",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["run_claim"],
                ttl_refreshed_by=("claim_execution", "renew_claim"),
                ttl_removed_by=("release_claim",),
                recovery_source="worker_reclaim",
                reconstructable=True,
                reconstruction_tested=True,
            ),
            model.make_observation(
                state_family="dag_task_state",
                key_pattern="hfa:dag:task:<task_id>:state",
                authority_class="execution_truth",
                durability_class="retained_execution_truth",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["dag_task_state"],
                ttl_refreshed_by=("task_admit", "task_dispatch", "task_claim", "task_complete"),
                ttl_removed_by=("task_requeue_plain_set",),
                expiry_observed=pttls["dag_task_state"]["after_accelerated_expiry"] == -2,
                history_survives_expiry=pttls["event_store_list"]["after_accelerated_expiry"] == -1,
                recovery_source="transport_and_optional_event_history_insufficient",
                reconstructable=False,
                reconstruction_tested=True,
                notes=("task completion emits no transition record", "requeue removes state TTL"),
            ),
            model.make_observation(
                state_family="dag_task_meta",
                key_pattern="hfa:dag:task:<task_id>:meta",
                authority_class="execution_truth",
                durability_class="retained_execution_truth",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["dag_task_meta"],
                ttl_refreshed_by=("task_admit", "task_dispatch", "task_claim", "task_complete"),
                expiry_observed=pttls["dag_task_meta"]["after_accelerated_expiry"] == -2,
                recovery_source="unknown",
                reconstructable=False,
                reconstruction_tested=True,
                notes=("heartbeat updates fields but does not refresh TTL",),
            ),
            model.make_observation(
                state_family="task_output",
                key_pattern="hfa:dag:task:<task_id>:output",
                authority_class="execution_truth",
                durability_class="retained_execution_truth",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["task_output"],
                ttl_refreshed_by=("task_complete",),
                expiry_observed=pttls["task_output"]["after_accelerated_expiry"] == -2,
                recovery_source="external_payload_store_only_for_large_payloads",
                reconstructable=False,
                reconstruction_tested=True,
            ),
            model.make_observation(
                state_family="remaining_dependencies",
                key_pattern="hfa:dag:task:<task_id>:remaining_deps",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["remaining_dependencies"],
                ttl_refreshed_by=("task_admit",),
                recovery_source="dag_definition",
                reconstructable=True,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="ready_emitted",
                key_pattern="hfa:dag:task:<task_id>:ready_emitted",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["ready_emitted"],
                ttl_refreshed_by=("task_admit", "child_unlock"),
                recovery_source="task_state_and_dependency_count",
                reconstructable=True,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="ready_queue",
                key_pattern="hfa:dag:tenant:<tenant_id>:ready",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["ready_queue"],
                ttl_refreshed_by=("task_admit",),
                ttl_removed_by=("task_requeue_recreates_zset_without_expire",),
                recovery_source="task_state_scan_not_tested",
                reconstructable=None,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="scheduled_zset",
                key_pattern="hfa:dag:tenant:<tenant_id>:scheduled",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["scheduled_zset"],
                ttl_refreshed_by=("task_dispatch",),
                recovery_source="task_state_scan",
                reconstructable=True,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="running_zset",
                key_pattern="hfa:dag:tenant:<tenant_id>:running",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="no_expiry",
                pttl_by_transition=pttls["running_zset"],
                ttl_refreshed_by=(),
                recovery_source="task_meta_heartbeat_scan",
                reconstructable=True,
                reconstruction_tested=False,
                notes=("heartbeat refreshes score, not key TTL",),
            ),
            model.make_observation(
                state_family="worker_reservation",
                key_pattern="hfa:dag:worker:<worker_id>:reservation",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["worker_reservation"],
                ttl_refreshed_by=("reserve", "renew"),
                ttl_removed_by=("claim_consumes_reservation",),
                expiry_observed=pttls["worker_reservation"]["after_accelerated_expiry"] == -2,
                recovery_source="scheduler_reservation_retry",
                reconstructable=True,
                reconstruction_tested=lifecycle["reservation_recreated_after_expiry"] == "reservation_created",
            ),
            model.make_observation(
                state_family="reservation_owner",
                key_pattern="hfa:dag:task:<task_id>:reservation_owner",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["reservation_owner"],
                ttl_refreshed_by=("reserve", "renew"),
                ttl_removed_by=("claim_consumes_owner",),
                expiry_observed=pttls["reservation_owner"]["after_accelerated_expiry"] == -2,
                recovery_source="scheduler_reservation_retry",
                reconstructable=True,
                reconstruction_tested=True,
            ),
            model.make_observation(
                state_family="shard_lease",
                key_pattern="hfa:cp:shard:owner:<shard>",
                authority_class="coordination_state",
                durability_class="ephemeral_coordination",
                retention_mechanism="expire_ttl",
                pttl_by_transition=pttls["shard_lease"],
                ttl_refreshed_by=("claim_shard", "renew_shard"),
                expiry_observed=pttls["shard_lease"]["after_accelerated_expiry"] == -2,
                recovery_source="worker_reclaim_and_orphan_monitor",
                reconstructable=True,
                reconstruction_tested=bool(lifecycle["shard_reclaimed_after_expiry"]),
            ),
            model.make_observation(
                state_family="shard_owner_projection",
                key_pattern="hfa:cp:shard:owners",
                authority_class="projection",
                durability_class="reconstructable_projection",
                retention_mechanism="no_expiry",
                pttl_by_transition=pttls["shard_owner_projection"],
                recovery_source="lease_keys_plus_orphan_monitor",
                reconstructable=True,
                reconstruction_tested=lifecycle["shard_projection_after_cleanup"] == "",
            ),
            model.make_observation(
                state_family="event_store_list",
                key_pattern="hfa:events:<run_id>",
                authority_class="audit",
                durability_class="audit_history",
                retention_mechanism="list_retention_unbounded",
                pttl_by_transition=pttls["event_store_list"],
                history_survives_expiry=pttls["event_store_list"]["after_accelerated_expiry"] == -1,
                recovery_source="self",
                reconstructable=False,
                reconstruction_tested=True,
                notes=("history survives but is not revision-bound or transaction-coupled",),
            ),
            model.make_observation(
                state_family="control_stream",
                key_pattern="control stream",
                authority_class="transport",
                durability_class="transport_retention",
                retention_mechanism="stream_maxlen_approximate",
                pttl_by_transition=pttls["control_stream"],
                recovery_source="none",
                reconstructable=False,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="shard_stream",
                key_pattern="shard stream",
                authority_class="transport",
                durability_class="transport_retention",
                retention_mechanism="stream_maxlen_approximate",
                pttl_by_transition=pttls["shard_stream"],
                recovery_source="none",
                reconstructable=False,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="completion_stream",
                key_pattern="completion stream",
                authority_class="runtime_notification",
                durability_class="transport_retention",
                retention_mechanism="stream_maxlen_approximate",
                pttl_by_transition=pttls["completion_stream"],
                recovery_source="none",
                reconstructable=False,
                reconstruction_tested=False,
            ),
            model.make_observation(
                state_family="worker_heartbeat_stream",
                key_pattern=HEARTBEAT_STREAM,
                authority_class="coordination_state",
                durability_class="transport_retention",
                retention_mechanism="stream_maxlen_approximate",
                pttl_by_transition=pttls["worker_heartbeat_stream"],
                recovery_source="next_worker_heartbeat",
                reconstructable=True,
                reconstruction_tested=True,
            ),
            model.make_observation(
                state_family="operator_audit_stream",
                key_pattern=TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
                authority_class="audit",
                durability_class="durable_truth_candidate",
                retention_mechanism="stream_maxlen_approximate",
                pttl_by_transition=pttls["operator_audit_stream"],
                recovery_source="none",
                reconstructable=False,
                reconstruction_tested=False,
                notes=("approximate MAXLEN=10000", "no external durable archive observed"),
            ),
        ]

        report = model.render_report(observations, lifecycle=lifecycle)
        model.write_json(repo_root / "local_out/sprint80/ttl_durability.json", report)
        return report
    finally:
        StateStore.RUN_META_TTL_SECONDS = old_run_meta_ttl
        StateStore.RUN_STATE_TTL_SECONDS = old_run_state_ttl
        StateStore.CLAIM_TTL = old_claim_ttl
        shard_module.OWNER_TTL = old_owner_ttl
        await redis.flushdb()
        await redis.aclose()


@pytest.fixture(scope="module")
def ttl_durability_report(
    repo_root: Path,
    sprint80_module_loader: Callable[[str], ModuleType],
):
    redis_url = os.getenv("SPRINT80_REDIS_URL", "")
    if not redis_url:
        pytest.skip("SPRINT80_REDIS_URL is required for TTL durability diagnostics")
    return asyncio.run(
        _build_report(
            redis_url=redis_url,
            repo_root=repo_root,
            model=sprint80_module_loader("ttl_durability"),
        )
    )


def _observation(report: dict[str, Any], family: str) -> dict[str, Any]:
    return next(row for row in report["observations"] if row["state_family"] == family)


def _pttls(row: dict[str, Any]) -> dict[str, int]:
    return {stage: int(value) for stage, value in row["pttl_by_transition"]}


@pytest.mark.sprint80_reality
def test_exact_ttl_state_families_are_observed(ttl_durability_report: dict[str, Any]):
    assert {row["state_family"] for row in ttl_durability_report["observations"]} == EXPECTED_STATE_FAMILIES


@pytest.mark.sprint80_reality
def test_pttl_semantics_use_absent_no_expiry_or_milliseconds(ttl_durability_report: dict[str, Any]):
    for row in ttl_durability_report["observations"]:
        for _stage, value in row["pttl_by_transition"]:
            assert int(value) >= -2


@pytest.mark.sprint80_reality
def test_lifecycle_transitions_refresh_task_state_and_meta_ttl(ttl_durability_report: dict[str, Any]):
    state = _pttls(_observation(ttl_durability_report, "dag_task_state"))
    meta = _pttls(_observation(ttl_durability_report, "dag_task_meta"))
    for stage in ("after_admit", "after_dispatch", "after_claim", "after_complete"):
        assert state[stage] > 0
        assert meta[stage] > 0


@pytest.mark.sprint80_reality
def test_heartbeat_does_not_extend_state_or_meta_retention(ttl_durability_report: dict[str, Any]):
    state = _pttls(_observation(ttl_durability_report, "dag_task_state"))
    meta = _pttls(_observation(ttl_durability_report, "dag_task_meta"))
    assert state["after_heartbeat"] <= state["before_heartbeat"] + 100
    assert meta["after_heartbeat"] <= meta["before_heartbeat"] + 100
    assert state["after_heartbeat"] < MAIN_TTL_SECONDS * 1000 - 300
    assert meta["after_heartbeat"] < MAIN_TTL_SECONDS * 1000 - 300


@pytest.mark.sprint80_reality
def test_requeue_removes_state_and_ready_queue_expiry(ttl_durability_report: dict[str, Any]):
    state = _pttls(_observation(ttl_durability_report, "dag_task_state"))
    ready = _pttls(_observation(ttl_durability_report, "ready_queue"))
    meta = _pttls(_observation(ttl_durability_report, "dag_task_meta"))
    assert state["requeue_before"] > 0
    assert state["requeue_after"] == -1
    assert ready["requeue_after"] == -1
    assert meta["requeue_after"] > 0


@pytest.mark.sprint80_reality
def test_completed_task_truth_expires_while_history_survives(ttl_durability_report: dict[str, Any]):
    state = _pttls(_observation(ttl_durability_report, "dag_task_state"))
    meta = _pttls(_observation(ttl_durability_report, "dag_task_meta"))
    output = _pttls(_observation(ttl_durability_report, "task_output"))
    history = _pttls(_observation(ttl_durability_report, "event_store_list"))
    assert state["after_accelerated_expiry"] == -2
    assert meta["after_accelerated_expiry"] == -2
    assert output["after_accelerated_expiry"] == -2
    assert history["after_accelerated_expiry"] == -1


@pytest.mark.sprint80_reality
def test_run_state_meta_and_result_expire(ttl_durability_report: dict[str, Any]):
    for family in ("run_state", "run_meta", "run_result"):
        values = _pttls(_observation(ttl_durability_report, family))
        assert values["after_accelerated_expiry"] == -2


@pytest.mark.sprint80_reality
def test_ephemeral_coordination_expires_and_can_be_recreated(ttl_durability_report: dict[str, Any]):
    reservation = _observation(ttl_durability_report, "worker_reservation")
    shard = _observation(ttl_durability_report, "shard_lease")
    assert _pttls(reservation)["after_accelerated_expiry"] == -2
    assert _pttls(shard)["after_accelerated_expiry"] == -2
    assert reservation["reconstructable"] is True
    assert reservation["reconstruction_tested"] is True
    assert shard["reconstructable"] is True
    assert shard["reconstruction_tested"] is True


@pytest.mark.sprint80_reality
def test_stream_retention_is_not_ttl(ttl_durability_report: dict[str, Any]):
    for family in (
        "control_stream",
        "shard_stream",
        "completion_stream",
        "worker_heartbeat_stream",
        "operator_audit_stream",
    ):
        row = _observation(ttl_durability_report, family)
        assert row["retention_mechanism"] == "stream_maxlen_approximate"
        assert all(value == -1 for _stage, value in row["pttl_by_transition"] if value != -2)


@pytest.mark.sprint80_reality
def test_operator_audit_is_approximate_maxlen_without_external_archive(ttl_durability_report: dict[str, Any]):
    audit = _observation(ttl_durability_report, "operator_audit_stream")
    assert audit["retention_mechanism"] == "stream_maxlen_approximate"
    assert audit["blocking_finding"] is True
    assert audit["recovery_source"] == "none"


@pytest.mark.sprint80_reality
def test_blocking_findings_are_explicit(ttl_durability_report: dict[str, Any]):
    findings = set(ttl_durability_report["blocking_findings"])
    assert "run_state" in findings
    assert "dag_task_state" in findings
    assert "operator_audit_stream" in findings


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Canonical state candidates expire without a proven reconstruction source",
)
def test_canonical_candidates_with_ttl_have_proven_recovery(ttl_durability_report: dict[str, Any]):
    candidates = [
        row
        for row in ttl_durability_report["observations"]
        if row["authority_class"] == "canonical_candidate"
    ]
    assert candidates
    assert all(row["reconstructable"] is True and row["reconstruction_tested"] for row in candidates)


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Expired task state/output cannot be reconstructed as exact terminal truth from current history",
)
def test_expired_task_terminal_truth_is_exactly_reconstructable(ttl_durability_report: dict[str, Any]):
    task_state = _observation(ttl_durability_report, "dag_task_state")
    task_output = _observation(ttl_durability_report, "task_output")
    assert task_state["reconstructable"] is True
    assert task_state["reconstruction_tested"] is True
    assert task_output["reconstructable"] is True
    assert task_output["reconstruction_tested"] is True


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Dedicated audit history uses approximate MAXLEN and no external durable archive is observed",
)
def test_durable_audit_history_has_non_trimming_archive(ttl_durability_report: dict[str, Any]):
    audit = _observation(ttl_durability_report, "operator_audit_stream")
    assert audit["retention_mechanism"] != "stream_maxlen_approximate"
    assert audit["reconstructable"] is True
