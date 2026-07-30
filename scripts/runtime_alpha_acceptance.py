from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
for relative in ("hfa-core/src", "hfa-control/src", "hfa-worker/src"):
    candidate = ROOT / relative
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import redis.asyncio as redis_async

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey, DagTaskDispatchInput, DagTaskSeed
from hfa_control.dag_lua import DagLua
from hfa_control.models import ControlPlaneConfig
from hfa_control.service import ControlPlaneService
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor


DEFAULT_OUTPUT = ROOT / "local_out" / "sprint83" / "runtime_alpha_acceptance.json"
_ACCEPTANCE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def _decode(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_mapping(raw: dict[Any, Any]) -> dict[str, str]:
    return {_decode(key): _decode(value) for key, value in (raw or {}).items()}


def _json_object(value: object) -> dict[str, Any]:
    text = _decode(value)
    if not text:
        return {}
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("expected a JSON object")
    return parsed


def _validate_acceptance_id(value: str) -> str:
    candidate = str(value or "").strip()
    if not _ACCEPTANCE_ID.fullmatch(candidate):
        raise ValueError(
            "acceptance_id must match [A-Za-z0-9][A-Za-z0-9_-]{0,63}"
        )
    return candidate


class AlphaEchoExecutor(TaskExecutor):
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        self.calls += 1
        prompt = str((ctx.payload or {}).get("prompt", ""))
        return TaskExecutionResult(
            ok=True,
            output={
                "task_id": ctx.task_id,
                "run_id": ctx.run_id,
                "message": prompt,
                "output_text": f"ALPHA_ECHO: {prompt}",
            },
        )


async def _pending_count(redis_client, stream: str) -> int:
    pending = await redis_client.xpending(stream, CONSUMER_GROUP)
    if isinstance(pending, dict):
        raw = pending.get("pending", pending.get(b"pending", 0))
        return int(raw or 0)
    if isinstance(pending, (tuple, list)) and pending:
        return int(pending[0] or 0)
    return int(pending or 0)


async def _wait_for_task_state(
    redis_client,
    *,
    task_id: str,
    expected: str,
    timeout_seconds: float = 5.0,
) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    observed = ""
    while loop.time() < deadline:
        observed = _decode(await redis_client.get(DagRedisKey.task_state(task_id)))
        if observed == expected:
            return observed
        await asyncio.sleep(0.02)
    raise TimeoutError(
        f"task state did not converge: task_id={task_id} "
        f"expected={expected} observed={observed}"
    )


async def _seed_run_authority(
    redis_client,
    *,
    run_id: str,
    tenant_id: str,
    state: str,
) -> None:
    """Seed test-only RUN authority for the disposable acceptance database."""

    await redis_client.set(RedisKey.run_state(run_id), state)
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "runtime-alpha-echo",
            "worker_group": "runtime-alpha-workers",
            "shard": "0",
            "reschedule_count": "0",
            "admitted_at": "1000",
            "state": state,
        },
    )


async def _build_runtime(redis_client, *, worker_id: str):
    dag = DagLua(redis_client)
    await dag.initialise()

    reservation = WorkerReservationManager(
        redis_client,
        reservation_ttl_seconds=30,
        scheduler_id="runtime-alpha-scheduler",
    )
    claim = TaskClaimManager(dag)
    heartbeat = TaskHeartbeatManager(redis_client)
    executor = AlphaEchoExecutor()
    task_consumer = TaskConsumer(
        claim,
        executor,
        heartbeat_manager=heartbeat,
        heartbeat_interval_ms=50,
        completion_manager=dag,
    )
    worker = WorkerConsumer(
        redis_client,
        worker_id,
        "runtime-alpha-workers",
        [0],
        executor,
        reclaim_idle_ms=100,
        task_consumer=task_consumer,
    )
    await worker.prepare_consumer_groups()
    return dag, reservation, executor, worker


async def _healthy_scenario(redis_client, *, acceptance_id: str) -> dict[str, Any]:
    tenant_id = f"tenant-alpha-{acceptance_id}"
    run_id = f"run-alpha-{acceptance_id}-success"
    task_id = f"task-alpha-{acceptance_id}-success"
    worker_id = f"worker-alpha-{acceptance_id}"
    scheduler_epoch = f"epoch-alpha-{acceptance_id}"
    message = "runtime alpha acceptance"
    payload = {"prompt": message}

    await _seed_run_authority(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        state="running",
    )
    dag, reservation, executor, worker = await _build_runtime(
        redis_client,
        worker_id=worker_id,
    )

    admitted = await dag.task_admit(
        DagTaskSeed(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type="runtime-alpha-echo",
            worker_group="runtime-alpha-workers",
            priority=5,
            admitted_at=1000.0,
            dependency_count=0,
            input_payload=payload,
            payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
            region="test",
            policy="LEAST_LOADED",
        )
    )
    if not admitted.admitted or not admitted.ready:
        raise RuntimeError(f"task admission failed: {admitted.status}")

    reserved = await reservation.reserve(
        worker_id=worker_id,
        task_id=task_id,
        scheduler_epoch=scheduler_epoch,
        reserved_at_ms=1500,
    )
    if not reserved.ok:
        raise RuntimeError(f"worker reservation failed: {reserved.status}")

    dispatch = await dag.task_dispatch_commit(
        DagTaskDispatchInput(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            worker_id=worker_id,
            worker_group="runtime-alpha-workers",
            agent_type="runtime-alpha-echo",
            shard=0,
            priority=5,
            admitted_at=1000.0,
            scheduled_at=2000.0,
            payload=payload,
            payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
            region="test",
            policy="LEAST_LOADED",
            scheduler_epoch=scheduler_epoch,
        )
    )
    if not dispatch.committed:
        raise RuntimeError(
            f"task dispatch failed: status={dispatch.status} reason={dispatch.reason}"
        )

    stream = RedisKey.stream_shard(0)
    await worker.start()
    try:
        await _wait_for_task_state(
            redis_client,
            task_id=task_id,
            expected="done",
        )
    finally:
        await worker.close()

    output = _json_object(await redis_client.get(DagRedisKey.task_output(task_id)))
    pending_count = await _pending_count(redis_client, stream)
    task_meta = _decode_mapping(
        await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    )
    control = ControlPlaneService(
        redis_client,
        ControlPlaneConfig(instance_id=f"runtime-alpha-query-{acceptance_id}"),
    )
    run_view = await control.get_run_state(run_id)

    task_completed = _decode(
        await redis_client.get(DagRedisKey.task_state(task_id))
    ) == "done"
    output_readable = output.get("output_text") == f"ALPHA_ECHO: {message}"
    message_acknowledged = pending_count == 0
    run_finalization_gap = bool(run_view.get("truth_conflict")) and any(
        row.get("detail_code") == "task_terminal_run_nonterminal"
        for row in run_view.get("truth_conflicts", [])
    )

    return {
        "name": "canonical_task_success",
        "status": "PASS" if task_completed and output_readable and message_acknowledged else "FAIL",
        "tenant_id": tenant_id,
        "run_id": run_id,
        "task_id": task_id,
        "task_admitted": admitted.admitted,
        "task_ready": admitted.ready,
        "reservation_created": reserved.ok,
        "dispatch_committed": dispatch.committed,
        "executor_calls": executor.calls,
        "task_completed": task_completed,
        "task_output_readable": output_readable,
        "output": output,
        "message_acknowledged": message_acknowledged,
        "pending_count": pending_count,
        "claim_epoch_present": bool(task_meta.get("claim_epoch")),
        "scheduler_epoch": task_meta.get("scheduler_epoch", ""),
        "run_state": run_view.get("state"),
        "run_truth_status": run_view.get("truth_status"),
        "run_truth_conflicts": run_view.get("truth_conflicts", []),
        "run_finalization_gap": run_finalization_gap,
    }


async def _terminal_dispatch_conflict_scenario(
    redis_client,
    *,
    acceptance_id: str,
) -> dict[str, Any]:
    tenant_id = f"tenant-alpha-{acceptance_id}"
    run_id = f"run-alpha-{acceptance_id}-terminal"
    task_id = f"task-alpha-{acceptance_id}-terminal"
    payload = {"prompt": "must not dispatch"}

    await _seed_run_authority(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        state="done",
    )
    dag = DagLua(redis_client)
    await dag.initialise()
    admitted = await dag.task_admit(
        DagTaskSeed(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type="runtime-alpha-echo",
            priority=5,
            admitted_at=3000.0,
            dependency_count=0,
            input_payload=payload,
            payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
        )
    )
    if not admitted.admitted or not admitted.ready:
        raise RuntimeError(f"conflict task admission failed: {admitted.status}")

    shard_stream = RedisKey.stream_shard(0)
    before = {
        "task_state": _decode(
            await redis_client.get(DagRedisKey.task_state(task_id))
        ),
        "ready_score": await redis_client.zscore(
            DagRedisKey.tenant_ready_queue(tenant_id), task_id
        ),
        "scheduled_score": await redis_client.zscore(
            DagRedisKey.task_scheduled_zset(tenant_id), task_id
        ),
        "stream_len": await redis_client.xlen(shard_stream),
    }

    result = await dag.task_dispatch_commit(
        DagTaskDispatchInput(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            worker_group="runtime-alpha-workers",
            agent_type="runtime-alpha-echo",
            shard=0,
            priority=5,
            admitted_at=3000.0,
            scheduled_at=4000.0,
            payload=payload,
            payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
            scheduler_epoch=f"epoch-alpha-{acceptance_id}",
        )
    )

    after = {
        "task_state": _decode(
            await redis_client.get(DagRedisKey.task_state(task_id))
        ),
        "ready_score": await redis_client.zscore(
            DagRedisKey.tenant_ready_queue(tenant_id), task_id
        ),
        "scheduled_score": await redis_client.zscore(
            DagRedisKey.task_scheduled_zset(tenant_id), task_id
        ),
        "stream_len": await redis_client.xlen(shard_stream),
    }

    index = _decode_mapping(
        await redis_client.hgetall(RedisKey.runtime_truth_conflict_index())
    )
    matching_conflicts: list[dict[str, Any]] = []
    for conflict_id, raw_payload in index.items():
        if conflict_id == "__runtime_truth_conflict_count":
            continue
        try:
            payload_object = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue
        if payload_object.get("task_id") == task_id:
            matching_conflicts.append(payload_object)

    no_lifecycle_mutation = before == after
    explained = any(
        row.get("operation") == "TASK_DISPATCH"
        and row.get("status") == "run_truth_terminal_conflict"
        and row.get("detail_code") == "run_state_terminal"
        for row in matching_conflicts
    )

    return {
        "name": "terminal_run_blocks_dispatch",
        "status": (
            "PASS"
            if not result.committed
            and result.status == "run_truth_terminal_conflict"
            and no_lifecycle_mutation
            and explained
            else "FAIL"
        ),
        "tenant_id": tenant_id,
        "run_id": run_id,
        "task_id": task_id,
        "dispatch_committed": result.committed,
        "dispatch_status": result.status,
        "dispatch_reason": result.reason,
        "no_lifecycle_mutation": no_lifecycle_mutation,
        "before": before,
        "after": after,
        "conflict_explained": explained,
        "conflicts": matching_conflicts,
    }


async def build_acceptance_report(
    redis_url: str,
    *,
    acceptance_id: str = "s83-1",
    reset_test_db: bool = False,
) -> dict[str, Any]:
    acceptance_id = _validate_acceptance_id(acceptance_id)
    redis_client = redis_async.from_url(redis_url, decode_responses=True)
    try:
        await redis_client.ping()
        if reset_test_db:
            await redis_client.flushdb()

        healthy = await _healthy_scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )
        conflict = await _terminal_dispatch_conflict_scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )

        scenario_pass = healthy["status"] == "PASS" and conflict["status"] == "PASS"
        runtime_alpha_testable = scenario_pass
        product_alpha_ready = False
        limitations = [
            "RUN finalization is not coordinated with terminal TASK completion",
            "TASK_CANCEL product command is not included in Sprint 83.1",
            "user-facing retry/requeue product command is not included in Sprint 83.1",
            "the harness uses a disposable Redis test database and a deterministic echo executor",
            "production feature flags and external provider execution remain disabled",
        ]

        return {
            "schema_version": 1,
            "sprint": "83.1",
            "source": "runtime_alpha_product_acceptance",
            "status": "PASS_WITH_LIMITATIONS" if scenario_pass else "FAIL",
            "acceptance_id": acceptance_id,
            "redis_url": redis_url,
            "redis_db_reset": reset_test_db,
            "runtime_alpha_testable": runtime_alpha_testable,
            "product_alpha_ready": product_alpha_ready,
            "production_ready": False,
            "global_truth_policy": "OPERATION_SCOPED_FAIL_CLOSED",
            "one_command_acceptance_available": True,
            "real_redis_used": True,
            "canonical_task_admit_used": True,
            "runtime_truth_guarded_dispatch_used": True,
            "worker_consumer_used": True,
            "task_consumer_used": True,
            "task_output_readable": bool(healthy.get("task_output_readable")),
            "conflict_explanation_available": bool(conflict.get("conflict_explained")),
            "run_finalization_supported": False,
            "cancel_command_supported": False,
            "retry_command_supported": False,
            "external_executor_enabled": False,
            "automatic_repair_authorized": False,
            "production_cutover_authorized": False,
            "scenarios": [healthy, conflict],
            "limitations": limitations,
        }
    finally:
        await redis_client.aclose()


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


async def _async_main(args: argparse.Namespace) -> int:
    report = await build_acceptance_report(
        args.redis_url,
        acceptance_id=args.acceptance_id,
        reset_test_db=args.reset_test_db,
    )
    output_path = Path(args.out)
    write_report(output_path, report)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(
            "RUNTIME_ALPHA_ACCEPTANCE "
            f"status={report['status']} "
            f"runtime_alpha_testable={str(report['runtime_alpha_testable']).lower()} "
            f"product_alpha_ready={str(report['product_alpha_ready']).lower()} "
            f"report={output_path}"
        )

    return 0 if report["status"] == "PASS_WITH_LIMITATIONS" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Sprint 83.1 disposable real-Redis runtime alpha product "
            "acceptance harness."
        )
    )
    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6389/15",
        help="Disposable Redis database URL. DB 15 is the default.",
    )
    parser.add_argument(
        "--acceptance-id",
        default="s83-1",
        help="Deterministic acceptance identity used in generated task/run IDs.",
    )
    parser.add_argument(
        "--reset-test-db",
        action="store_true",
        help="FLUSHDB before the run. Use only with a disposable test database.",
    )
    parser.add_argument(
        "--out",
        default=str(DEFAULT_OUTPUT),
        help="JSON report output path.",
    )
    parser.add_argument("--json", action="store_true", help="Print the report as JSON.")
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
