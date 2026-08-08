from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

for relative in (
    "hfa-core/src",
    "hfa-control/src",
    "hfa-worker/src",
):
    candidate = ROOT / relative
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import redis.asyncio as redis_async

from hfa.config.keys import RedisKey
from hfa.dag.schema import (
    DagRedisKey,
    DagTaskDispatchInput,
    DagTaskSeed,
)
from hfa_control.shard import ShardOwnershipManager
from hfa_control.worker_reservation import (
    WorkerReservationManager,
)
from hfa_control.run_terminal_event_evidence import ensure_terminal_event_index
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService
from hfa_worker.process_root import config_from_env

from scripts.runtime_alpha_acceptance import (
    _decode,
    _decode_mapping,
    _validate_acceptance_id,
    write_report,
)


DEFAULT_OUTPUT = (
    ROOT
    / "local_out"
    / "sprint83"
    / "sprint83_4_production_run_finalization_binding.json"
)


class ProductionBindingExecutor:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def execute(self, event):
        self.calls.append(
            {
                "run_id": str(
                    getattr(event, "run_id", "") or ""
                ),
                "task_id": str(
                    getattr(event, "task_id", "") or ""
                ),
                "tenant_id": str(
                    getattr(event, "tenant_id", "") or ""
                ),
            }
        )
        return SimpleNamespace(
            status="done",
            payload={
                "output_text":
                    "SPRINT83_4_PRODUCTION_BINDING_OK",
            },
            error="",
        )


async def _wait_for_run_state(
    redis_client,
    *,
    run_id: str,
    expected: str,
    timeout_seconds: float = 8.0,
) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    observed = ""

    while loop.time() < deadline:
        observed = _decode(
            await redis_client.get(
                RedisKey.run_state(run_id)
            )
        )
        if observed == expected:
            return observed
        await asyncio.sleep(0.02)

    raise TimeoutError(
        "RUN state did not converge: "
        f"run_id={run_id} "
        f"expected={expected} "
        f"observed={observed}"
    )


async def _pending_count(
    redis_client,
    stream: str,
) -> int:
    pending = await redis_client.xpending(
        stream,
        CONSUMER_GROUP,
    )

    if isinstance(pending, dict):
        raw = pending.get(
            "pending",
            pending.get(b"pending", 0),
        )
        return int(raw or 0)

    if isinstance(pending, (tuple, list)) and pending:
        return int(pending[0] or 0)

    return int(pending or 0)


async def _result_events(
    redis_client,
    *,
    run_id: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for _entry_id, fields in await redis_client.xrange(
        RedisKey.stream_results()
    ):
        decoded = _decode_mapping(fields)
        if decoded.get("run_id") == run_id:
            rows.append(decoded)

    return rows


def _worker_config(
    redis_client,
    *,
    worker_id: str,
    worker_group: str,
    enabled: bool,
    executor: object,
) -> dict[str, Any]:
    return {
        "production": True,
        "worker_id": worker_id,
        "worker_group": worker_group,
        "region": "sprint83-4-acceptance",
        "shards": [0],
        "capacity": 1,
        "version": "83.4-acceptance",
        "capabilities": ["base"],
        "executor": executor,
        "shard_manager": ShardOwnershipManager(
            redis_client,
            object(),
        ),
        "shard_renew_interval": 1.0,
        "task_heartbeat_interval_ms": 50,
        "task_stale_after_ms": 5_000,
        "run_termination_binding_enabled": enabled,
    }


async def _production_binding_scenario(
    redis_client,
    *,
    acceptance_id: str,
) -> dict[str, Any]:
    worker_id = f"worker-s83-4-{acceptance_id}"
    worker_group = f"group-s83-4-{acceptance_id}"
    tenant_id = f"tenant-s83-4-{acceptance_id}"
    run_id = f"run-s83-4-{acceptance_id}"
    task_id = f"task-s83-4-{acceptance_id}"
    scheduler_epoch = f"epoch-s83-4-{acceptance_id}"

    payload = {
        "prompt":
            "Sprint 83.4 production composition binding",
    }
    stream = RedisKey.stream_shard(0)

    default_env = config_from_env(
        {
            "WORKER_EXECUTOR_MODE": "fake",
            "WORKER_ID":
                f"worker-default-s83-4-{acceptance_id}",
        }
    )
    enabled_env = config_from_env(
        {
            "WORKER_EXECUTOR_MODE": "fake",
            "WORKER_ID":
                f"worker-enabled-s83-4-{acceptance_id}",
            "WORKER_RUN_TERMINATION_BINDING": "true",
        }
    )

    disabled_executor = ProductionBindingExecutor()
    disabled = WorkerService(
        redis_client,
        _worker_config(
            redis_client,
            worker_id=(
                f"worker-disabled-s83-4-{acceptance_id}"
            ),
            worker_group=worker_group,
            enabled=False,
            executor=disabled_executor,
        ),
    )

    disabled_task_consumer_class = type(
        disabled._task_consumer
    ).__name__
    disabled_worker_consumer_class = type(
        disabled._consumer
    ).__name__
    disabled_coordinator_absent = (
        disabled._run_termination_coordinator is None
    )
    await disabled.close(drain_timeout=0)

    await redis_client.set(
        RedisKey.run_state(run_id),
        "running",
    )
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "state": "running",
            "worker_group": worker_group,
            "shard": "0",
        },
    )
    await redis_client.zadd(
        RedisKey.cp_running(),
        {run_id: 1000},
    )

    executor = ProductionBindingExecutor()
    worker = WorkerService(
        redis_client,
        _worker_config(
            redis_client,
            worker_id=worker_id,
            worker_group=worker_group,
            enabled=True,
            executor=executor,
        ),
    )
    reservation = WorkerReservationManager(
        redis_client,
        reservation_ttl_seconds=30,
        scheduler_id=(
            f"scheduler-s83-4-{acceptance_id}"
        ),
    )

    enabled_task_consumer_class = type(
        worker._task_consumer
    ).__name__
    enabled_worker_consumer_class = type(
        worker._consumer
    ).__name__
    completion_manager_class = type(
        worker._run_termination_coordinator
    ).__name__

    await worker.start()
    try:
        admitted = await worker._dag_lua.task_admit(
            DagTaskSeed(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                agent_type="sprint83-4-acceptance",
                worker_group=worker_group,
                priority=5,
                admitted_at=1000.0,
                dependency_count=0,
                input_payload=payload,
                required_capabilities=[],
                region="sprint83-4-acceptance",
                policy="LEAST_LOADED",
                payload_json=json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        )

        reserved = await reservation.reserve(
            worker_id=worker_id,
            task_id=task_id,
            scheduler_epoch=scheduler_epoch,
            reserved_at_ms=1500,
        )

        dispatched = (
            await worker._dag_lua.task_dispatch_commit(
                DagTaskDispatchInput(
                    task_id=task_id,
                    run_id=run_id,
                    tenant_id=tenant_id,
                    worker_id=worker_id,
                    worker_group=worker_group,
                    agent_type="sprint83-4-acceptance",
                    shard=0,
                    priority=5,
                    admitted_at=1000.0,
                    scheduled_at=2000.0,
                    payload=payload,
                    payload_json=json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    region="sprint83-4-acceptance",
                    policy="LEAST_LOADED",
                    scheduler_epoch=scheduler_epoch,
                )
            )
        )

        if not admitted.admitted or not admitted.ready:
            raise RuntimeError(
                f"TASK_ADMIT failed: {admitted.status}"
            )
        if not reserved.ok:
            raise RuntimeError(
                f"worker reservation failed: {reserved.status}"
            )
        if not dispatched.committed:
            raise RuntimeError(
                "TASK_DISPATCH failed: "
                f"{dispatched.status}"
            )

        await _wait_for_run_state(
            redis_client,
            run_id=run_id,
            expected="done",
        )

        task_state = _decode(
            await redis_client.get(
                DagRedisKey.task_state(task_id)
            )
        )
        output = json.loads(
            _decode(
                await redis_client.get(
                    DagRedisKey.task_output(task_id)
                )
            )
        )
        run_state = _decode(
            await redis_client.get(
                RedisKey.run_state(run_id)
            )
        )
        run_meta = _decode_mapping(
            await redis_client.hgetall(
                RedisKey.run_meta(run_id)
            )
        )
        run_result = _decode_mapping(
            await redis_client.hgetall(
                RedisKey.run_result(run_id)
            )
        )
        result_payload = json.loads(
            run_result.get("payload") or "{}"
        )
        events = await _result_events(
            redis_client,
            run_id=run_id,
        )
        pending_count = await _pending_count(
            redis_client,
            stream,
        )
        running_projection_cleared = (
            await redis_client.zscore(
                RedisKey.cp_running(),
                run_id,
            )
            is None
        )

        passed = all(
            (
                default_env[
                    "run_termination_binding_enabled"
                ]
                is False,
                enabled_env[
                    "run_termination_binding_enabled"
                ]
                is True,
                disabled_task_consumer_class
                == "TaskConsumer",
                disabled_worker_consumer_class
                == "WorkerConsumer",
                disabled_coordinator_absent,
                enabled_task_consumer_class
                == "RunFinalizingTaskConsumer",
                enabled_worker_consumer_class
                == "RunFinalizingWorkerConsumer",
                completion_manager_class
                == "RunTerminationCoordinator",
                task_state == "done",
                run_state == "done",
                output.get("output_text")
                == "SPRINT83_4_PRODUCTION_BINDING_OK",
                run_meta.get("state") == "done",
                run_meta.get("finalization_operation")
                == "RUN_TERMINATE",
                run_result.get("status") == "done",
                run_result.get("finalization_operation")
                == "RUN_TERMINATE",
                run_result.get("trigger_task_id")
                == task_id,
                result_payload.get("task_count") == 1,
                result_payload.get("done_count") == 1,
                len(events) == 1,
                events[0].get("event_type")
                == "RunCompleted",
                pending_count == 0,
                running_projection_cleared,
                len(executor.calls) == 1,
            )
        )

        return {
            "name":
                "production_worker_enabled_binding_finalizes_run",
            "status": "PASS" if passed else "FAIL",
            "worker_id": worker_id,
            "tenant_id": tenant_id,
            "run_id": run_id,
            "task_id": task_id,
            "flag_default_enabled": default_env[
                "run_termination_binding_enabled"
            ],
            "flag_test_enabled": enabled_env[
                "run_termination_binding_enabled"
            ],
            "disabled_task_consumer_class":
                disabled_task_consumer_class,
            "disabled_worker_consumer_class":
                disabled_worker_consumer_class,
            "disabled_coordinator_absent":
                disabled_coordinator_absent,
            "enabled_task_consumer_class":
                enabled_task_consumer_class,
            "enabled_worker_consumer_class":
                enabled_worker_consumer_class,
            "completion_manager_class":
                completion_manager_class,
            "task_state": task_state,
            "run_state": run_state,
            "run_result_status":
                run_result.get("status", ""),
            "run_finalization_operation":
                run_result.get(
                    "finalization_operation",
                    "",
                ),
            "terminal_event_count": len(events),
            "terminal_event_type": (
                events[0].get("event_type", "")
                if events
                else ""
            ),
            "pending_count": pending_count,
            "running_projection_cleared":
                running_projection_cleared,
            "executor_call_count": len(executor.calls),
            "task_count": result_payload.get(
                "task_count",
                0,
            ),
            "done_count": result_payload.get(
                "done_count",
                0,
            ),
            "failed_count": result_payload.get(
                "failed_count",
                0,
            ),
        }
    finally:
        await worker.close(drain_timeout=0)


async def build_acceptance_report(
    redis_url: str,
    *,
    acceptance_id: str = "s83-4",
    reset_test_db: bool = False,
) -> dict[str, Any]:
    acceptance_id = _validate_acceptance_id(
        acceptance_id
    )
    redis_client = redis_async.from_url(
        redis_url,
        decode_responses=True,
    )

    try:
        await redis_client.ping()
        if reset_test_db:
            await redis_client.flushdb()
        await ensure_terminal_event_index(redis_client)

        scenario = await _production_binding_scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )
        passed = scenario["status"] == "PASS"

        return {
            "schema_version": 1,
            "sprint": "83.4",
            "source":
                "production_run_finalization_binding_acceptance",
            "status": (
                "PASS_WITH_LIMITATIONS"
                if passed
                else "FAIL"
            ),
            "acceptance_id": acceptance_id,
            "redis_url": redis_url,
            "redis_db_reset": reset_test_db,
            "one_command_acceptance_available": True,
            "real_redis_used": True,
            "production_worker_composition_supported":
                passed,
            "run_termination_binding_supported": passed,
            "run_termination_binding_default_enabled":
                False,
            "run_termination_binding_test_enabled": True,
            "opt_in_end_to_end_run_terminalization":
                passed,
            "canonical_lifecycle_authority_changed":
                False,
            "new_lifecycle_writer": False,
            "Loop_Plane_dependencies": 0,
            "product_alpha_ready": False,
            "production_ready": False,
            "cancel_command_supported": False,
            "retry_command_supported": False,
            "automatic_repair_authorized": False,
            "production_cutover_authorized": False,
            "scenarios": [scenario],
            "limitations": [
                "The production RUN termination binding remains default disabled.",
                "The acceptance uses a disposable Redis database and deterministic executor.",
                "RUN cancellation and user-facing retry commands are outside Sprint 83.4.",
                "Automatic reconciliation and production cutover remain unauthorized.",
                "A terminal TASK heartbeat may observe an illegal transition and stop fail-closed.",
            ],
        }
    finally:
        await redis_client.aclose()


async def _async_main(
    args: argparse.Namespace,
) -> int:
    report = await build_acceptance_report(
        args.redis_url,
        acceptance_id=args.acceptance_id,
        reset_test_db=args.reset_test_db,
    )
    output = Path(args.out)
    write_report(output, report)

    if args.json:
        print(
            json.dumps(
                report,
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(
            "SPRINT83_4_ACCEPTANCE "
            f"status={report['status']} "
            "production_worker_composition_supported="
            f"{str(report['production_worker_composition_supported']).lower()} "
            f"report={output}"
        )

    return 0 if report["status"] != "FAIL" else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run Sprint 83.4 production RUN "
            "finalization binding acceptance"
        )
    )
    parser.add_argument(
        "--redis-url",
        default="redis://127.0.0.1:6389/0",
    )
    parser.add_argument(
        "--acceptance-id",
        default="s83-4",
    )
    parser.add_argument(
        "--reset-test-db",
        action="store_true",
    )
    parser.add_argument(
        "--out",
        default=str(DEFAULT_OUTPUT),
    )
    parser.add_argument(
        "--json",
        action="store_true",
    )
    args = parser.parse_args()
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
