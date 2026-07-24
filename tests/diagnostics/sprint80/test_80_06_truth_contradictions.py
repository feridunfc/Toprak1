from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Callable

import pytest
import redis.asyncio as redis_asyncio

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.api.router import run_state as api_run_state
from hfa_control.models import ControlPlaneConfig
from hfa_control.recovery import RecoveryService
from hfa_control.service import ControlPlaneService
from hfa_control.task_recovery import TaskRecoveryManager
from hfa_worker.idempotency import IdempotencyGuard
from hfa_worker.runtime.terminal_duplicate_delivery import (
    NOT_TERMINAL_DUPLICATE_DELIVERY,
    TERMINAL_DUPLICATE_DELIVERY,
    classify_terminal_duplicate_delivery,
)
from hfa_worker.task_context import TaskContext

EXPECTED_CALLERS = {
    "control_api_run_state",
    "run_recovery_missing_state",
    "run_recovery_terminal_guard",
    "task_recovery_missing_state",
    "worker_task_duplicate_guard_nonterminal",
    "worker_task_duplicate_guard_terminal",
    "legacy_worker_run_guard",
    "scheduler_dag_dispatch",
}


def _load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load diagnostic module: {path}")
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


async def _seed_run(
    redis,
    *,
    run_id: str,
    tenant_id: str,
    state: str | None,
) -> None:
    if state is not None:
        await redis.set(RedisKey.run_state(run_id), state, ex=86400)
    await redis.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "python",
            "worker_group": "s80-group",
            "shard": "0",
            "reschedule_count": "0",
            "admitted_at": "1",
            "state": state or "",
        },
    )


async def _seed_task(
    redis,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    state: str | None,
    heartbeat_at_ms: int | None = None,
) -> None:
    if state is not None:
        await redis.set(DagRedisKey.task_state(task_id), state, ex=86400)
    mapping: dict[str, str] = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
    }
    if heartbeat_at_ms is not None:
        mapping["last_heartbeat_at_ms"] = str(heartbeat_at_ms)
    await redis.hset(DagRedisKey.task_meta(task_id), mapping=mapping)


def _ctx(*, task_id: str, run_id: str, tenant_id: str) -> TaskContext:
    return TaskContext(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="python",
        worker_group="s80-group",
        worker_instance_id="s80-worker",
        payload={"prompt": "truth contradiction"},
        shard=0,
    )


async def _observe_api(
    redis,
    *,
    model: ModuleType,
    scenario: str,
    run_state: str,
    task_state: str,
) -> Any:
    await redis.flushdb()
    run_id = f"s80-truth-{scenario}-run"
    task_id = f"s80-truth-{scenario}-task"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state=run_state)
    await _seed_task(
        redis,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        state=task_state,
    )

    cp = ControlPlaneService(redis, ControlPlaneConfig(instance_id=f"s80-{scenario}"))
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(cp=cp, redis=redis))
    )
    response = await api_run_state(run_id, request, x_tenant_id=tenant_id)

    return model.make_observation(
        scenario=scenario,
        run_state=run_state,
        task_state=task_state,
        caller="control_api_run_state",
        selected_truth_source="run_state_and_meta",
        returned_status=response.state,
        mutation_attempted=False,
        fail_open_or_closed="not_applicable",
        deterministic_winner="RUN_WINS",
        notes=(
            "API route delegates to ControlPlaneService.get_run_state",
            "DAG task state is not read",
        ),
    )


async def _observe_run_recovery_missing_state(redis, *, model: ModuleType) -> Any:
    await redis.flushdb()
    run_id = "s80-truth-run-missing"
    task_id = "s80-truth-task-terminal"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state=None)
    await _seed_task(
        redis,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        state="done",
    )
    config = ControlPlaneConfig(instance_id="s80-run-recovery", stale_run_timeout=1.0)
    await redis.zadd(config.running_zset, {run_id: 0})
    recovery = RecoveryService(redis, config)
    stale = await recovery._find_stale_runs()
    zset_retained = await redis.zscore(config.running_zset, run_id) is not None

    return model.make_observation(
        scenario="run_missing_task_terminal",
        run_state="missing",
        task_state="done",
        caller="run_recovery_missing_state",
        selected_truth_source="running_zset_plus_run_state",
        returned_status="no_candidate_missing_run_state",
        mutation_attempted=False,
        fail_open_or_closed="fail_closed",
        deterministic_winner="FAIL_CLOSED",
        recovery_action="none_stale_zset_entry_retained",
        notes=(
            f"candidate_count={len(stale)}",
            f"running_zset_entry_retained={zset_retained}",
            "terminal task state is ignored by run recovery",
        ),
    )


async def _observe_run_recovery_terminal_guard(redis, *, model: ModuleType) -> Any:
    await redis.flushdb()
    run_id = "s80-truth-run-done-recovery"
    task_id = "s80-truth-task-running-recovery"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state="done")
    await _seed_task(
        redis,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        state="running",
        heartbeat_at_ms=1,
    )
    config = ControlPlaneConfig(instance_id="s80-run-terminal", stale_run_timeout=1.0)
    await redis.zadd(config.running_zset, {run_id: 0})
    recovery = RecoveryService(redis, config)
    stale = await recovery._find_stale_runs()
    zset_removed = await redis.zscore(config.running_zset, run_id) is None

    return model.make_observation(
        scenario="run_terminal_task_running_recovery",
        run_state="done",
        task_state="running",
        caller="run_recovery_terminal_guard",
        selected_truth_source="running_zset_plus_run_state",
        returned_status="terminal_run_not_candidate",
        mutation_attempted=zset_removed,
        fail_open_or_closed="fail_closed",
        deterministic_winner="RUN_WINS",
        recovery_action="remove_terminal_run_from_running_zset",
        notes=(
            f"candidate_count={len(stale)}",
            f"running_zset_entry_removed={zset_removed}",
            "running task state is ignored by run recovery",
        ),
    )


async def _observe_task_recovery_missing_state(redis, *, model: ModuleType) -> Any:
    await redis.flushdb()
    run_id = "s80-truth-run-terminal-task-missing"
    task_id = "s80-truth-task-missing"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state="done")
    await redis.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1})

    manager = TaskRecoveryManager(redis)
    stale = await manager.find_stale_tasks(tenant_id=tenant_id, now_ms=100_000)
    result = await manager.requeue_stale_task(
        task_id=task_id,
        tenant_id=tenant_id,
        expected_state="running",
        now_ms=100_000,
        ready_score=100_000,
    )
    ready_score = await redis.zscore(DagRedisKey.tenant_ready_queue(tenant_id), task_id)

    return model.make_observation(
        scenario="task_missing_run_terminal",
        run_state="done",
        task_state="missing",
        caller="task_recovery_missing_state",
        selected_truth_source="task_running_zset_plus_task_meta_then_task_state",
        returned_status=f"candidate={task_id in stale};requeue={result.status}",
        mutation_attempted=True,
        fail_open_or_closed="fail_closed",
        deterministic_winner="FAIL_CLOSED",
        recovery_action="requeue_blocked_by_missing_task_state",
        notes=(
            f"ready_queue_score={ready_score}",
            "terminal run state is ignored by task recovery",
            "missing heartbeat makes the task a stale candidate before state CAS blocks mutation",
        ),
    )


async def _observe_duplicate_guard_nonterminal(redis, *, model: ModuleType) -> Any:
    await redis.flushdb()
    run_id = "s80-truth-run-done-task-running"
    task_id = "s80-truth-task-running"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state="done")
    await _seed_task(
        redis,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        state="running",
    )
    decision = await classify_terminal_duplicate_delivery(
        redis,
        _ctx(task_id=task_id, run_id=run_id, tenant_id=tenant_id),
        message_task_id=task_id,
        message_run_id=run_id,
    )

    return model.make_observation(
        scenario="run_done_task_running_duplicate_guard",
        run_state="done",
        task_state="running",
        caller="worker_task_duplicate_guard_nonterminal",
        selected_truth_source="task_state_and_meta",
        returned_status=decision.status,
        mutation_attempted=False,
        fail_open_or_closed="fail_open",
        deterministic_winner="TASK_WINS",
        recovery_action="allow_downstream_claim_path",
        notes=(
            f"suppress_execution={decision.suppress_execution}",
            "terminal run state is not read",
        ),
    )


async def _observe_duplicate_guard_terminal(redis, *, model: ModuleType) -> Any:
    await redis.flushdb()
    run_id = "s80-truth-run-running-task-done"
    task_id = "s80-truth-task-done"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state="running")
    await _seed_task(
        redis,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        state="done",
    )
    decision = await classify_terminal_duplicate_delivery(
        redis,
        _ctx(task_id=task_id, run_id=run_id, tenant_id=tenant_id),
        message_task_id=task_id,
        message_run_id=run_id,
    )

    return model.make_observation(
        scenario="run_running_task_done_worker_guard",
        run_state="running",
        task_state="done",
        caller="worker_task_duplicate_guard_terminal",
        selected_truth_source="task_state_and_meta",
        returned_status=decision.status,
        mutation_attempted=False,
        fail_open_or_closed="fail_closed",
        deterministic_winner="TASK_WINS",
        recovery_action="suppress_claim_execution_completion_and_allow_ack",
        notes=(
            f"ack_allowed={decision.ack_allowed}",
            f"ack_policy={decision.ack_policy}",
            "nonterminal run state is not read",
        ),
    )


async def _observe_legacy_worker_guard(redis, *, model: ModuleType) -> Any:
    await redis.flushdb()
    run_id = "s80-truth-legacy-run-done"
    task_id = "s80-truth-legacy-task-running"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state="done")
    await _seed_task(
        redis,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        state="running",
    )
    should_execute = await IdempotencyGuard(redis).should_execute(run_id)

    return model.make_observation(
        scenario="run_done_task_running_legacy_worker_guard",
        run_state="done",
        task_state="running",
        caller="legacy_worker_run_guard",
        selected_truth_source="run_state",
        returned_status=f"should_execute={str(should_execute).lower()}",
        mutation_attempted=False,
        fail_open_or_closed="fail_closed",
        deterministic_winner="RUN_WINS",
        recovery_action="suppress_legacy_execution",
        notes=(
            "legacy RunRequested path uses IdempotencyGuard.should_execute",
            "running DAG task state is not read",
        ),
    )


async def _observe_scheduler_dispatch(
    redis,
    *,
    repo_root: Path,
    model: ModuleType,
) -> Any:
    await redis.flushdb()
    base = _load_module(
        repo_root / "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py",
        "sprint80_truth_scheduler_cardinality",
    )
    run_id = "s80-truth-scheduler-run-done"
    task_id = "s80-truth-scheduler-task-ready"
    tenant_id = "s80-tenant"
    await _seed_run(redis, run_id=run_id, tenant_id=tenant_id, state="done")
    keys = base._task_keys(task_id, tenant_id, run_id)
    keys["control_stream"] = "s80:truth:control"
    keys["shard_stream"] = "s80:truth:shard:0"
    await base._admit(redis, repo_root, keys, task_id, run_id, tenant_id)
    before = _decode(await redis.get(keys["state"]))
    result = await base._dispatch(redis, repo_root, keys, task_id, run_id, tenant_id)
    after = _decode(await redis.get(keys["state"]))
    persisted_run_state = _decode(await redis.get(RedisKey.run_state(run_id)))

    return model.make_observation(
        scenario="run_terminal_task_ready_scheduler",
        run_state=persisted_run_state,
        task_state=before,
        caller="scheduler_dag_dispatch",
        selected_truth_source="task_state",
        returned_status=f"{_decode(result[0])}:{before}->{after}",
        mutation_attempted=before != after,
        fail_open_or_closed="fail_open",
        deterministic_winner="TASK_WINS",
        recovery_action="dispatch_committed_despite_terminal_run",
        notes=(
            f"run_state_after_dispatch={persisted_run_state}",
            "task_dispatch.lua does not read run state",
        ),
    )


async def _build_report(
    *,
    redis_url: str,
    repo_root: Path,
    model: ModuleType,
) -> dict[str, Any]:
    redis = redis_asyncio.Redis.from_url(redis_url, decode_responses=False)
    await redis.ping()
    try:
        observations = [
            await _observe_api(
                redis,
                model=model,
                scenario="api_run_done_task_running",
                run_state="done",
                task_state="running",
            ),
            await _observe_api(
                redis,
                model=model,
                scenario="api_run_running_task_done",
                run_state="running",
                task_state="done",
            ),
            await _observe_run_recovery_missing_state(redis, model=model),
            await _observe_run_recovery_terminal_guard(redis, model=model),
            await _observe_task_recovery_missing_state(redis, model=model),
            await _observe_duplicate_guard_nonterminal(redis, model=model),
            await _observe_duplicate_guard_terminal(redis, model=model),
            await _observe_legacy_worker_guard(redis, model=model),
            await _observe_scheduler_dispatch(
                redis,
                repo_root=repo_root,
                model=model,
            ),
        ]
        report = model.render_report(
            observations,
            metadata={
                "method": "real_redis_entry_point_observation",
                "run_api_reads_task_state": False,
                "run_recovery_reads_task_state": False,
                "task_recovery_reads_run_state": False,
                "worker_task_guard_reads_run_state": False,
                "legacy_worker_guard_reads_task_state": False,
                "scheduler_dispatch_reads_run_state": False,
            },
        )
        model.write_json(
            repo_root / "local_out/sprint80/truth_contradictions.json",
            report,
        )
        return report
    finally:
        await redis.flushdb()
        await redis.aclose()


@pytest.fixture(scope="module")
def truth_contradiction_report(
    repo_root: Path,
    sprint80_module_loader: Callable[[str], ModuleType],
) -> dict[str, Any]:
    redis_url = os.getenv("SPRINT80_REDIS_URL", "")
    if not redis_url:
        pytest.skip("SPRINT80_REDIS_URL is required for truth contradiction diagnostics")
    return asyncio.run(
        _build_report(
            redis_url=redis_url,
            repo_root=repo_root,
            model=sprint80_module_loader("truth_contradictions"),
        )
    )


def _scenario(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(row for row in report["observations"] if row["scenario"] == name)


@pytest.mark.sprint80_reality
def test_api_read_when_run_done_and_task_running(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(truth_contradiction_report, "api_run_done_task_running")
    assert row["returned_status"] == "done"
    assert row["selected_truth_source"] == "run_state_and_meta"
    assert row["deterministic_winner"] == "RUN_WINS"


@pytest.mark.sprint80_reality
def test_api_read_when_run_running_and_task_done(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(truth_contradiction_report, "api_run_running_task_done")
    assert row["returned_status"] == "running"
    assert row["selected_truth_source"] == "run_state_and_meta"
    assert row["deterministic_winner"] == "RUN_WINS"


@pytest.mark.sprint80_reality
def test_recovery_when_run_missing_and_task_terminal(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(truth_contradiction_report, "run_missing_task_terminal")
    assert row["returned_status"] == "no_candidate_missing_run_state"
    assert row["deterministic_winner"] == "FAIL_CLOSED"
    assert row["mutation_attempted"] is False


@pytest.mark.sprint80_reality
def test_recovery_when_task_missing_and_run_terminal(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(truth_contradiction_report, "task_missing_run_terminal")
    assert "candidate=True" in row["returned_status"]
    assert "TASK_STATE_CONFLICT" in row["returned_status"]
    assert row["deterministic_winner"] == "FAIL_CLOSED"


@pytest.mark.sprint80_reality
def test_duplicate_suppression_when_run_and_task_disagree(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(
        truth_contradiction_report,
        "run_done_task_running_duplicate_guard",
    )
    assert row["returned_status"] == NOT_TERMINAL_DUPLICATE_DELIVERY
    assert row["deterministic_winner"] == "TASK_WINS"
    assert row["fail_open_or_closed"] == "fail_open"


@pytest.mark.sprint80_reality
def test_scheduler_guard_when_run_terminal_and_task_ready(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(truth_contradiction_report, "run_terminal_task_ready_scheduler")
    assert row["returned_status"].startswith("committed:ready->scheduled")
    assert row["mutation_attempted"] is True
    assert row["deterministic_winner"] == "TASK_WINS"


@pytest.mark.sprint80_reality
def test_worker_guard_when_task_terminal_and_run_running(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(
        truth_contradiction_report,
        "run_running_task_done_worker_guard",
    )
    assert row["returned_status"] == TERMINAL_DUPLICATE_DELIVERY
    assert row["deterministic_winner"] == "TASK_WINS"
    assert row["fail_open_or_closed"] == "fail_closed"


@pytest.mark.sprint80_reality
def test_legacy_worker_guard_when_run_terminal_and_task_running(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(
        truth_contradiction_report,
        "run_done_task_running_legacy_worker_guard",
    )
    assert row["returned_status"] == "should_execute=false"
    assert row["deterministic_winner"] == "RUN_WINS"


@pytest.mark.sprint80_reality
def test_run_recovery_when_run_terminal_and_task_running(
    truth_contradiction_report: dict[str, Any],
):
    row = _scenario(
        truth_contradiction_report,
        "run_terminal_task_running_recovery",
    )
    assert row["deterministic_winner"] == "RUN_WINS"
    assert row["mutation_attempted"] is True
    assert row["recovery_action"] == "remove_terminal_run_from_running_zset"


@pytest.mark.sprint80_reality
def test_each_caller_reports_selected_truth_source(
    truth_contradiction_report: dict[str, Any],
):
    callers = {row["caller"] for row in truth_contradiction_report["observations"]}
    assert callers == EXPECTED_CALLERS
    assert all(row["selected_truth_source"] for row in truth_contradiction_report["observations"])
    assert truth_contradiction_report["global_truth_policy"] == "INCONSISTENT_BY_CALLER"


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Current callers select run truth, task truth, or fail closed independently",
)
def test_all_callers_share_one_deterministic_truth_rule(
    truth_contradiction_report: dict[str, Any],
):
    assert truth_contradiction_report["global_truth_policy"] in {
        "RUN_WINS",
        "TASK_WINS",
    }
    assert len(truth_contradiction_report["selected_truth_source_counts"]) == 1


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Scheduler task dispatch can commit while the compatibility run state is terminal",
)
def test_terminal_truth_in_either_plane_blocks_conflicting_mutation(
    truth_contradiction_report: dict[str, Any],
):
    contradictory_terminal_rows = [
        row
        for row in truth_contradiction_report["observations"]
        if (row["run_state"] in {"done", "failed", "dead_lettered"})
        != (row["task_state"] in {"done", "failed", "dead_lettered", "blocked_by_failure"})
    ]
    assert contradictory_terminal_rows
    assert all(row["mutation_attempted"] is False for row in contradictory_terminal_rows)
