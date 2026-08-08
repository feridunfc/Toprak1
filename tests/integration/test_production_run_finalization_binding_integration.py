from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import (
    DagRedisKey,
    DagTaskDispatchInput,
    DagTaskSeed,
)
from hfa_control.shard import ShardOwnershipManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_control.run_terminal_event_evidence import ensure_terminal_event_index
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService

from scripts.runtime_alpha_acceptance_83_4 import (
    build_acceptance_report,
)


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


class SuccessfulExecutor:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def execute(self, event):
        self.calls.append(
            {
                "run_id": str(getattr(event, "run_id", "") or ""),
                "task_id": str(getattr(event, "task_id", "") or ""),
                "tenant_id": str(
                    getattr(event, "tenant_id", "") or ""
                ),
            }
        )

        class Result:
            status = "done"
            payload = {
                "output_text": "SPRINT83_4_PRODUCTION_BINDING_OK",
            }
            error = ""

        return Result()


async def _wait_until(
    predicate,
    *,
    timeout: float = 8.0,
    interval: float = 0.05,
):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_error: BaseException | None = None

    while loop.time() < deadline:
        try:
            value = await predicate()
            if value:
                return value
        except BaseException as exc:
            last_error = exc
        await asyncio.sleep(interval)

    if last_error is not None:
        raise AssertionError(
            "Timed out waiting for Sprint 83.4 production "
            f"binding condition: {last_error}"
        ) from last_error

    raise AssertionError(
        "Timed out waiting for Sprint 83.4 production "
        "binding condition"
    )


async def _run_is_done(redis_client, run_id: str) -> bool:
    return await redis_client.get(
        RedisKey.run_state(run_id)
    ) == "done"


async def _pending_count(redis_client, stream: str) -> int:
    pending = await redis_client.xpending(stream, CONSUMER_GROUP)

    if isinstance(pending, dict):
        return int(
            pending.get(
                "pending",
                pending.get(b"pending", 0),
            )
            or 0
        )

    if isinstance(pending, (tuple, list)) and pending:
        return int(pending[0] or 0)

    return int(pending or 0)


async def _result_events(
    redis_client,
    *,
    run_id: str,
) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []

    for _entry_id, fields in await redis_client.xrange(
        RedisKey.stream_results()
    ):
        decoded = {
            (
                key.decode("utf-8", errors="replace")
                if isinstance(key, bytes)
                else str(key)
            ): (
                value.decode("utf-8", errors="replace")
                if isinstance(value, bytes)
                else str(value)
            )
            for key, value in fields.items()
        }

        if decoded.get("run_id") == run_id:
            events.append(decoded)

    return events


async def test_production_worker_enabled_binding_finalizes_run(
    redis_client,
) -> None:
    suffix = uuid.uuid4().hex[:10]

    worker_id = f"worker-s83-4-{suffix}"
    worker_group = f"group-s83-4-{suffix}"
    tenant_id = f"tenant-s83-4-{suffix}"
    run_id = f"run-s83-4-{suffix}"
    task_id = f"task-s83-4-{suffix}"
    scheduler_epoch = f"epoch-s83-4-{suffix}"

    shard = 0
    stream = RedisKey.stream_shard(shard)
    payload = {
        "prompt": "Sprint 83.4 production composition binding",
    }

    await ensure_terminal_event_index(redis_client)
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
            "shard": str(shard),
        },
    )
    await redis_client.zadd(
        RedisKey.cp_running(),
        {run_id: 1000},
    )

    shard_manager = ShardOwnershipManager(
        redis_client,
        object(),
    )
    executor = SuccessfulExecutor()

    worker = WorkerService(
        redis_client,
        {
            "production": True,
            "worker_id": worker_id,
            "worker_group": worker_group,
            "region": "integration",
            "shards": [shard],
            "capacity": 1,
            "version": "83.4-integration",
            "capabilities": ["base"],
            "executor": executor,
            "shard_manager": shard_manager,
            "shard_renew_interval": 1.0,
            "task_heartbeat_interval_ms": 50,
            "task_stale_after_ms": 5_000,
            "run_termination_binding_enabled": True,
        },
    )

    reservation = WorkerReservationManager(
        redis_client,
        reservation_ttl_seconds=30,
        scheduler_id="scheduler-s83-4-integration",
    )

    await worker.start()
    try:
        assert worker.is_ready is True
        assert worker.run_termination_binding_enabled is True
        assert worker._run_termination_coordinator is not None

        admitted = await worker._dag_lua.task_admit(
            DagTaskSeed(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                agent_type="integration",
                worker_group=worker_group,
                priority=5,
                admitted_at=1000.0,
                dependency_count=0,
                input_payload=payload,
                required_capabilities=[],
                region="integration",
                policy="LEAST_LOADED",
                payload_json=json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        )

        assert admitted.admitted is True
        assert admitted.ready is True

        reserved = await reservation.reserve(
            worker_id=worker_id,
            task_id=task_id,
            scheduler_epoch=scheduler_epoch,
            reserved_at_ms=1500,
        )

        assert reserved.ok is True

        dispatched = await worker._dag_lua.task_dispatch_commit(
            DagTaskDispatchInput(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                worker_id=worker_id,
                worker_group=worker_group,
                agent_type="integration",
                shard=shard,
                priority=5,
                admitted_at=1000.0,
                scheduled_at=2000.0,
                payload=payload,
                payload_json=json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                region="integration",
                policy="LEAST_LOADED",
                scheduler_epoch=scheduler_epoch,
            )
        )

        assert dispatched.committed is True

        await _wait_until(
            lambda: _run_is_done(redis_client, run_id)
        )

        assert await redis_client.get(
            DagRedisKey.task_state(task_id)
        ) == "done"

        task_output_raw = await redis_client.get(
            DagRedisKey.task_output(task_id)
        )
        assert task_output_raw not in (None, "", b"")

        task_output = json.loads(task_output_raw)
        assert (
            task_output["output_text"]
            == "SPRINT83_4_PRODUCTION_BINDING_OK"
        )

        assert executor.calls == [
            {
                "run_id": run_id,
                "task_id": task_id,
                "tenant_id": tenant_id,
            }
        ]

        run_meta = await redis_client.hgetall(
            RedisKey.run_meta(run_id)
        )
        assert run_meta["state"] == "done"
        assert (
            run_meta["finalization_operation"]
            == "RUN_TERMINATE"
        )
        assert (
            run_meta["finalization_source"]
            == "terminal_task_aggregate"
        )

        run_result = await redis_client.hgetall(
            RedisKey.run_result(run_id)
        )
        assert run_result["status"] == "done"
        assert (
            run_result["finalization_operation"]
            == "RUN_TERMINATE"
        )
        assert run_result["trigger_task_id"] == task_id

        result_payload = json.loads(run_result["payload"])
        assert result_payload["task_count"] == 1
        assert result_payload["done_count"] == 1
        assert result_payload["failed_count"] == 0

        events = await _result_events(
            redis_client,
            run_id=run_id,
        )
        assert len(events) == 1
        assert events[0]["event_type"] == "RunCompleted"
        assert (
            events[0]["finalization_operation"]
            == "RUN_TERMINATE"
        )

        assert await redis_client.zscore(
            RedisKey.cp_running(),
            run_id,
        ) is None

        assert await _pending_count(
            redis_client,
            stream,
        ) == 0
    finally:
        await worker.close(drain_timeout=0)



async def test_production_binding_acceptance_report() -> None:
    redis_url = os.environ.get(
        "REDIS_URL",
        "redis://127.0.0.1:6389/0",
    )
    report = await build_acceptance_report(
        redis_url,
        acceptance_id="s83-4-integration",
        reset_test_db=True,
    )

    assert report["schema_version"] == 1
    assert report["sprint"] == "83.4"
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert (
        report["production_worker_composition_supported"]
        is True
    )
    assert report["run_termination_binding_supported"] is True
    assert (
        report["run_termination_binding_default_enabled"]
        is False
    )
    assert (
        report["run_termination_binding_test_enabled"]
        is True
    )
    assert (
        report["opt_in_end_to_end_run_terminalization"]
        is True
    )
    assert (
        report["canonical_lifecycle_authority_changed"]
        is False
    )
    assert report["new_lifecycle_writer"] is False
    assert report["Loop_Plane_dependencies"] == 0
    assert report["product_alpha_ready"] is False
    assert report["production_ready"] is False
    assert report["production_cutover_authorized"] is False

    scenario = report["scenarios"][0]
    assert scenario["status"] == "PASS"
    assert (
        scenario["disabled_task_consumer_class"]
        == "TaskConsumer"
    )
    assert (
        scenario["disabled_worker_consumer_class"]
        == "WorkerConsumer"
    )
    assert (
        scenario["enabled_task_consumer_class"]
        == "RunFinalizingTaskConsumer"
    )
    assert (
        scenario["enabled_worker_consumer_class"]
        == "RunFinalizingWorkerConsumer"
    )
    assert (
        scenario["completion_manager_class"]
        == "RunTerminationCoordinator"
    )
    assert scenario["task_state"] == "done"
    assert scenario["run_state"] == "done"
    assert scenario["terminal_event_count"] == 1
    assert scenario["terminal_event_type"] == "RunCompleted"
    assert scenario["pending_count"] == 0
    assert scenario["running_projection_cleared"] is True


async def test_production_binding_report_is_deterministic() -> None:
    redis_url = os.environ.get(
        "REDIS_URL",
        "redis://127.0.0.1:6389/0",
    )

    first = await build_acceptance_report(
        redis_url,
        acceptance_id="s83-4-deterministic",
        reset_test_db=True,
    )
    second = await build_acceptance_report(
        redis_url,
        acceptance_id="s83-4-deterministic",
        reset_test_db=True,
    )

    assert first == second
    assert len(first["scenarios"]) == 1
    assert len(first["limitations"]) == 5


async def test_production_binding_one_command_cli(
    tmp_path: Path,
) -> None:
    output = tmp_path / "sprint83-4-acceptance.json"
    repo_root = Path(__file__).resolve().parents[2]
    redis_url = os.environ.get(
        "REDIS_URL",
        "redis://127.0.0.1:6389/0",
    )

    env = os.environ.copy()
    python_paths = [
        str(repo_root),
        str(repo_root / "hfa-core" / "src"),
        str(repo_root / "hfa-control" / "src"),
        str(repo_root / "hfa-worker" / "src"),
    ]
    if env.get("PYTHONPATH"):
        python_paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_paths)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/runtime_alpha_acceptance_83_4.py",
            "--redis-url",
            redis_url,
            "--acceptance-id",
            "s83-4-cli",
            "--reset-test-db",
            "--out",
            str(output),
            "--json",
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    stdout = json.loads(completed.stdout)
    written = json.loads(
        output.read_text(encoding="utf-8")
    )

    assert stdout == written
    assert written["status"] == "PASS_WITH_LIMITATIONS"
    assert (
        written["production_worker_composition_supported"]
        is True
    )
    assert written["production_ready"] is False
    assert (
        written["production_cutover_authorized"]
        is False
    )
