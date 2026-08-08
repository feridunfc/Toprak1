from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
for relative in ("hfa-core/src", "hfa-control/src", "hfa-worker/src"):
    candidate = ROOT / relative
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import redis.asyncio as redis_async

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey, DagTaskDispatchInput, DagTaskSeed
from hfa_control.dag_lua import DagLua
from hfa_control.run_termination import RunTerminationCoordinator
from hfa_control.run_terminal_event_evidence import ensure_terminal_event_index
from hfa_control.models import ControlPlaneConfig
from hfa_control.service import ControlPlaneService
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.run_finalizing_runtime import (
    RunFinalizingTaskConsumer,
    RunFinalizingWorkerConsumer,
)

from scripts.runtime_alpha_acceptance import (
    AlphaEchoExecutor,
    _decode,
    _decode_mapping,
    _json_object,
    _pending_count,
    _seed_run_authority,
    _terminal_dispatch_conflict_scenario,
    _validate_acceptance_id,
    _wait_for_task_state,
    write_report,
)


DEFAULT_OUTPUT = ROOT / "local_out" / "sprint83" / "runtime_alpha_acceptance_83_2.json"


async def _build_runtime(redis_client, *, worker_id: str):
    dag = DagLua(redis_client)
    await dag.initialise()
    completion = RunTerminationCoordinator(
        redis_client,
        dag,
        enabled=True,
    )
    await completion.initialise()

    reservation = WorkerReservationManager(
        redis_client,
        reservation_ttl_seconds=30,
        scheduler_id="runtime-alpha-scheduler-83-2",
    )
    claim = TaskClaimManager(dag)
    heartbeat = TaskHeartbeatManager(redis_client)
    executor = AlphaEchoExecutor()
    task_consumer = RunFinalizingTaskConsumer(
        claim,
        executor,
        heartbeat_manager=heartbeat,
        heartbeat_interval_ms=50,
        completion_manager=completion,
    )
    worker = RunFinalizingWorkerConsumer(
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


async def _wait_for_run_finalization_convergence(
    redis_client,
    *,
    run_id: str,
    stream: str,
    expected_state: str = "done",
    timeout_seconds: float = 5.0,
) -> None:
    """Wait for the full Sprint 83.2 completion boundary before worker shutdown.

    TASK state convergence alone is not sufficient: task_complete() writes the
    terminal TASK state before RUN_TERMINATE finishes and before the stream
    message is ACKed. Closing the worker at TASK=done therefore creates a
    timing race in the acceptance harness.
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    observed_state = ""
    observed_result: dict[str, str] = {}
    observed_pending = -1

    while loop.time() < deadline:
        observed_state = _decode(await redis_client.get(RedisKey.run_state(run_id)))
        observed_result = _decode_mapping(
            await redis_client.hgetall(RedisKey.run_result(run_id))
        )
        observed_pending = await _pending_count(redis_client, stream)

        if (
            observed_state == expected_state
            and observed_result.get("status") == expected_state
            and observed_result.get("finalization_operation") == "RUN_TERMINATE"
            and observed_result.get("finalization_source")
            == "terminal_task_aggregate"
            and observed_pending == 0
        ):
            return

        await asyncio.sleep(0.02)

    raise TimeoutError(
        "RUN finalization did not converge before worker shutdown: "
        f"run_id={run_id} expected_state={expected_state} "
        f"observed_state={observed_state} "
        f"result_status={observed_result.get('status', '')} "
        f"pending_count={observed_pending}"
    )


async def _healthy_finalization_scenario(
    redis_client,
    *,
    acceptance_id: str,
) -> dict[str, Any]:
    tenant_id = f"tenant-alpha-{acceptance_id}"
    run_id = f"run-alpha-{acceptance_id}-success"
    task_id = f"task-alpha-{acceptance_id}-success"
    worker_id = f"worker-alpha-{acceptance_id}"
    scheduler_epoch = f"epoch-alpha-{acceptance_id}"
    message = "runtime alpha acceptance with run finalization"
    payload = {"prompt": message}

    await _seed_run_authority(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        state="running",
    )
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 1000})
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
        await _wait_for_run_finalization_convergence(
            redis_client,
            run_id=run_id,
            stream=stream,
            expected_state="done",
        )
    finally:
        await worker.close()

    output = _json_object(await redis_client.get(DagRedisKey.task_output(task_id)))
    pending_count = await _pending_count(redis_client, stream)
    task_meta = _decode_mapping(
        await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    )
    run_meta = _decode_mapping(await redis_client.hgetall(RedisKey.run_meta(run_id)))
    run_result = _decode_mapping(await redis_client.hgetall(RedisKey.run_result(run_id)))
    if run_result.get("payload"):
        run_result["payload"] = json.loads(run_result["payload"])
    run_result_report = {
        key: value
        for key, value in run_result.items()
        if key not in {"completed_at", "finalized_at_ms"}
    }

    result_events: list[dict[str, str]] = []
    for _event_id, fields in await redis_client.xrange(RedisKey.stream_results()):
        decoded = _decode_mapping(fields)
        if decoded.get("run_id") == run_id:
            result_events.append(
                {
                    key: value
                    for key, value in decoded.items()
                    if key not in {"completed_at", "finalized_at_ms"}
                }
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
    run_finalized = (
        run_view.get("state") == "done"
        and run_view.get("truth_status") == "consistent"
        and run_view.get("truth_conflict") is False
    )
    result_visible = (
        run_result.get("status") == "done"
        and run_result.get("finalization_operation") == "RUN_TERMINATE"
        and run_result.get("finalization_source") == "terminal_task_aggregate"
        and run_result.get("trigger_task_id") == task_id
        and run_result.get("payload", {}).get("task_count") == 1
    )
    event_visible = (
        len(result_events) == 1
        and result_events[0].get("event_type") == "RunCompleted"
        and result_events[0].get("finalization_operation") == "RUN_TERMINATE"
        and result_events[0].get("trigger_task_id") == task_id
    )
    running_projection_cleared = (
        await redis_client.zscore(RedisKey.cp_running(), run_id) is None
    )
    meta_converged = (
        run_meta.get("state") == "done"
        and run_meta.get("finalization_operation") == "RUN_TERMINATE"
        and run_meta.get("finalization_source") == "terminal_task_aggregate"
    )

    passed = all(
        (
            task_completed,
            output_readable,
            message_acknowledged,
            run_finalized,
            result_visible,
            event_visible,
            running_projection_cleared,
            meta_converged,
        )
    )
    return {
        "name": "canonical_task_success_finalizes_run",
        "status": "PASS" if passed else "FAIL",
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
        "run_finalized": run_finalized,
        "run_result_visible": result_visible,
        "run_result": run_result_report,
        "run_terminal_event_visible": event_visible,
        "run_terminal_events": result_events,
        "running_projection_cleared": running_projection_cleared,
        "run_meta_converged": meta_converged,
    }


async def build_acceptance_report(
    redis_url: str,
    *,
    acceptance_id: str = "s83-2",
    reset_test_db: bool = False,
) -> dict[str, Any]:
    acceptance_id = _validate_acceptance_id(acceptance_id)
    redis_client = redis_async.from_url(redis_url, decode_responses=True)
    try:
        await redis_client.ping()
        if reset_test_db:
            await redis_client.flushdb()
        await ensure_terminal_event_index(redis_client)

        healthy = await _healthy_finalization_scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )
        conflict = await _terminal_dispatch_conflict_scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )
        scenario_pass = healthy["status"] == "PASS" and conflict["status"] == "PASS"
        limitations = [
            "TASK_CANCEL product command is not included in Sprint 83.2",
            "user-facing retry/requeue product command is not included in Sprint 83.2",
            "the harness uses a disposable Redis test database and a deterministic echo executor",
            "the RUN_TERMINATE binding remains explicit opt-in and production cutover is disabled",
        ]
        return {
            "schema_version": 2,
            "sprint": "83.2",
            "source": "runtime_alpha_product_acceptance",
            "status": "PASS_WITH_LIMITATIONS" if scenario_pass else "FAIL",
            "acceptance_id": acceptance_id,
            "redis_url": redis_url,
            "redis_db_reset": reset_test_db,
            "runtime_alpha_testable": scenario_pass,
            "product_alpha_ready": False,
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
            "run_finalization_supported": bool(healthy.get("run_finalized")),
            "run_result_visible": bool(healthy.get("run_result_visible")),
            "run_terminal_event_visible": bool(
                healthy.get("run_terminal_event_visible")
            ),
            "run_termination_binding_default_enabled": False,
            "run_termination_binding_test_enabled": True,
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
            "RUNTIME_ALPHA_ACCEPTANCE_83_2 "
            f"status={report['status']} "
            f"run_finalization_supported="
            f"{str(report['run_finalization_supported']).lower()} "
            f"product_alpha_ready={str(report['product_alpha_ready']).lower()} "
            f"report={output_path}"
        )
    return 0 if report["status"] == "PASS_WITH_LIMITATIONS" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Sprint 83.2 disposable real-Redis runtime alpha acceptance "
            "harness with opt-in terminal TASK to RUN finalization."
        )
    )
    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6389/15",
        help="Disposable Redis database URL. DB 15 is the default.",
    )
    parser.add_argument(
        "--acceptance-id",
        default="s83-2",
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
