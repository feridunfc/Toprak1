from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

for relative in (
    "hfa-core/src",
    "hfa-control/src",
    "hfa-worker/src",
    "hfa-tools/src",
):
    candidate = ROOT / relative
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import httpx
import redis.asyncio as redis_async
from fastapi import FastAPI

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.api.router import router
from hfa_control.models import ControlPlaneConfig
from hfa_control.run_submission import RunSubmissionCoordinator
from hfa_control.service import ControlPlaneService
from hfa_control.shard import ShardOwnershipManager
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService

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
    / "sprint83_5_user_facing_runtime_e2e.json"
)


class ProductPathExecutor:
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
                    "SPRINT83_5_USER_FACING_E2E_OK",
            },
            error="",
        )


class DeterministicUUIDFactory:
    def __init__(self, *values: UUID) -> None:
        self._values = iter(values)

    def __call__(self) -> UUID:
        return next(self._values)


def _uuid4_from_material(material: str) -> UUID:
    raw = bytearray(
        hashlib.sha256(material.encode("utf-8")).digest()[:16]
    )
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return UUID(bytes=bytes(raw))


async def _wait_until(
    predicate,
    *,
    description: str,
    timeout_seconds: float = 18.0,
    interval_seconds: float = 0.05,
):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    last_value: Any = None
    last_error: BaseException | None = None

    while loop.time() < deadline:
        try:
            last_value = await predicate()
            if last_value:
                return last_value
        except BaseException as exc:
            last_error = exc
        await asyncio.sleep(interval_seconds)

    detail = (
        f" last_error={last_error!r}"
        if last_error is not None
        else f" last_value={last_value!r}"
    )
    raise TimeoutError(
        f"Timed out waiting for {description}.{detail}"
    )


async def _runtime_ready(
    control: ControlPlaneService,
    *,
    worker_id: str,
) -> bool:
    workers = await control.list_schedulable_workers()
    worker_visible = any(
        str(getattr(worker, "worker_id", "") or "")
        == worker_id
        for worker in workers
    )
    return bool(
        control.is_leader
        and control._scheduler.running
        and worker_visible
    )


async def _terminal_http_view(
    client: httpx.AsyncClient,
    *,
    run_id: str,
    tenant_id: str,
):
    response = await client.get(
        f"/control/v1/runs/{run_id}",
        headers={"X-Tenant-ID": tenant_id},
    )
    if response.status_code != 200:
        return None
    payload = response.json()
    if (
        payload.get("status") == "COMPLETED"
        and payload.get("terminal") is True
    ):
        return payload
    return None


async def _pending_count(
    redis_client,
    *,
    stream: str,
) -> int:
    pending = await redis_client.xpending(
        stream,
        CONSUMER_GROUP,
    )
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
    result: list[dict[str, str]] = []
    for _entry_id, fields in await redis_client.xrange(
        RedisKey.stream_results()
    ):
        decoded = _decode_mapping(fields)
        if decoded.get("run_id") == run_id:
            result.append(decoded)
    return result


def _control_config(
    *,
    acceptance_id: str,
) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id=f"cp-s83-5-{acceptance_id}",
        region="sprint83-5-acceptance",
        stream_shards=1,
        worker_heartbeat_ttl=30.0,
        registry_ttl=60,
        leader_ttl=15,
        leader_renew_interval=1.0,
        scheduler_loop_max_dispatches=4,
        scheduler_loop_max_duration_ms=100,
        scheduler_loop_max_failures=8,
        scheduler_loop_idle_sleep_ms=20,
        scheduler_loop_error_sleep_ms=50,
        scheduler_reservation_ttl_seconds=30,
        strict_cas_mode=True,
    )


def _worker_config(
    redis_client,
    *,
    worker_id: str,
    worker_group: str,
    executor: ProductPathExecutor,
) -> dict[str, Any]:
    return {
        "production": True,
        "worker_id": worker_id,
        "worker_group": worker_group,
        "region": "sprint83-5-acceptance",
        "shards": [0],
        "capacity": 1,
        "version": "83.5-acceptance",
        "capabilities": ["base"],
        "executor": executor,
        "shard_manager": ShardOwnershipManager(
            redis_client,
            object(),
        ),
        "shard_renew_interval": 1.0,
        "task_heartbeat_interval_ms": 50,
        "task_stale_after_ms": 5_000,
        "run_termination_binding_enabled": True,
    }


async def _http_product_scenario(
    redis_client,
    *,
    acceptance_id: str,
) -> dict[str, Any]:
    tenant_id = f"tenant{acceptance_id.replace('-', '')}"
    worker_id = f"worker-s83-5-{acceptance_id}"
    worker_group = f"group-s83-5-{acceptance_id}"

    executor = ProductPathExecutor()
    control = ControlPlaneService(
        redis_client,
        _control_config(acceptance_id=acceptance_id),
    )
    control._run_submission = RunSubmissionCoordinator(
        admission_controller=control._admitter,
        dag_lua=control._scheduler.composition.dag_lua,
        uuid_factory=DeterministicUUIDFactory(
            _uuid4_from_material(
                f"{acceptance_id}:run"
            ),
            _uuid4_from_material(
                f"{acceptance_id}:task"
            ),
        ),
        clock_ms=lambda: 1_700_000_000_123,
    )
    worker = WorkerService(
        redis_client,
        _worker_config(
            redis_client,
            worker_id=worker_id,
            worker_group=worker_group,
            executor=executor,
        ),
    )

    app = FastAPI()
    app.include_router(router)
    app.state.cp = control
    app.state.redis = redis_client
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sprint83-5.test",
    )

    await control.start()
    try:
        await worker.start()
        try:
            await _wait_until(
                lambda: _runtime_ready(
                    control,
                    worker_id=worker_id,
                ),
                description=(
                    "production scheduler leadership "
                    "and schedulable worker"
                ),
            )

            submit = await client.post(
                "/control/v1/runs",
                headers={"X-Tenant-ID": tenant_id},
                json={
                    "payload": {
                        "prompt":
                            "Sprint 83.5 HTTP product path",
                    },
                    "agent_type":
                        "sprint83-5-acceptance",
                    "priority": 5,
                    "estimated_cost_cents": 0,
                    "preferred_region": "",
                    "preferred_placement":
                        "LEAST_LOADED",
                    "required_capabilities": [],
                    "trace_parent": "",
                    "trace_state": "",
                },
            )
            submit_body = submit.json()
            if submit.status_code != 202:
                raise RuntimeError(
                    "HTTP submission failed: "
                    f"status={submit.status_code} "
                    f"body={submit_body}"
                )

            run_id = str(submit_body["run_id"])
            task_id = str(submit_body["task_id"])

            terminal = await _wait_until(
                lambda: _terminal_http_view(
                    client,
                    run_id=run_id,
                    tenant_id=tenant_id,
                ),
                description="HTTP combined terminal RUN view",
            )

            task_state = _decode(
                await redis_client.get(
                    DagRedisKey.task_state(task_id)
                )
            )
            task_output = json.loads(
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
            run_result = _decode_mapping(
                await redis_client.hgetall(
                    RedisKey.run_result(run_id)
                )
            )
            aggregate_payload = (
                terminal.get("result") or {}
            ).get("payload") or {}
            events = await _result_events(
                redis_client,
                run_id=run_id,
            )
            pending_count = await _pending_count(
                redis_client,
                stream=RedisKey.stream_shard(0),
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
                    submit_body.get("status")
                    == "ACCEPTED",
                    submit_body.get("run_admitted")
                    is True,
                    submit_body.get("task_admitted")
                    is True,
                    submit_body.get("task_ready")
                    is True,
                    submit_body.get("dispatch_possible")
                    is True,
                    task_state == "done",
                    run_state == "done",
                    terminal.get("status")
                    == "COMPLETED",
                    terminal.get("outcome")
                    == "SUCCESS",
                    terminal.get("completeness")
                    == "TERMINAL_WITH_RESULT",
                    aggregate_payload.get("task_count")
                    == 1,
                    aggregate_payload.get("done_count")
                    == 1,
                    task_output.get("output_text")
                    == "SPRINT83_5_USER_FACING_E2E_OK",
                    run_result.get("status")
                    == "done",
                    len(events) == 1,
                    events[0].get("event_type")
                    == "RunCompleted",
                    pending_count == 0,
                    running_projection_cleared,
                    len(executor.calls) == 1,
                    control._scheduler.running,
                    worker.is_ready,
                )
            )

            return {
                "name":
                    "http_submit_scheduler_worker_terminal_read",
                "status": "PASS" if passed else "FAIL",
                "http_submit_status_code":
                    submit.status_code,
                "http_submit_status":
                    submit_body.get("status", ""),
                "tenant_id": tenant_id,
                "run_id": run_id,
                "task_id": task_id,
                "scheduler_running":
                    control._scheduler.running,
                "worker_ready": worker.is_ready,
                "worker_id": worker_id,
                "task_state": task_state,
                "run_state": run_state,
                "http_terminal_status":
                    terminal.get("status", ""),
                "http_terminal_outcome":
                    terminal.get("outcome", ""),
                "http_completeness":
                    terminal.get("completeness", ""),
                "aggregate_task_count":
                    aggregate_payload.get(
                        "task_count",
                        0,
                    ),
                "aggregate_done_count":
                    aggregate_payload.get(
                        "done_count",
                        0,
                    ),
                "task_output_text":
                    task_output.get(
                        "output_text",
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
                "executor_call_count":
                    len(executor.calls),
                "direct_lifecycle_write_used": False,
                "direct_dispatch_call_used": False,
            }
        finally:
            await worker.close(drain_timeout=0)
    finally:
        await client.aclose()
        await control._redis_monitor.close()
        await control.close()


async def build_acceptance_report(
    redis_url: str,
    *,
    acceptance_id: str = "s83-5",
    reset_test_db: bool = False,
) -> dict[str, Any]:
    acceptance_id = _validate_acceptance_id(
        acceptance_id
    )
    old_binding = os.environ.get(
        "HFA_CANONICAL_TASK_ADMIT_BINDING"
    )
    os.environ[
        "HFA_CANONICAL_TASK_ADMIT_BINDING"
    ] = "1"

    redis_client = redis_async.from_url(
        redis_url,
        decode_responses=False,
    )
    try:
        await redis_client.ping()
        if reset_test_db:
            await redis_client.flushdb()

        scenario = await _http_product_scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )
        passed = scenario["status"] == "PASS"
        return {
            "schema_version": 1,
            "sprint": "83.5",
            "source":
                "user_facing_runtime_e2e_acceptance",
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
            "user_facing_http_submit_supported":
                passed,
            "user_facing_http_status_result_supported":
                passed,
            "production_scheduler_used": passed,
            "production_worker_used": passed,
            "canonical_task_admit_used": passed,
            "run_finalization_supported": passed,
            "terminal_http_read_supported": passed,
            "task_output_durable": passed,
            "task_output_exposed_in_http_result": False,
            "new_lifecycle_writer": False,
            "direct_lifecycle_write_used": False,
            "direct_dispatch_call_used": False,
            "Loop_Plane_dependencies": 0,
            "product_alpha_ready": False,
            "production_ready": False,
            "external_executor_enabled": False,
            "cancel_command_supported": False,
            "retry_command_supported": False,
            "automatic_repair_authorized": False,
            "production_cutover_authorized": False,
            "scenarios": [scenario],
            "limitations": [
                "The acceptance uses an in-process ASGI transport, disposable Redis data, and a deterministic executor.",
                "The durable TASK output is proven but is not yet exposed in the combined HTTP RUN result.",
                "The production RUN termination binding remains an explicit worker opt-in.",
                "Cancellation, retry, automatic reconciliation, external executor cutover, and production cutover remain unauthorized.",
                "A terminal TASK heartbeat may observe an illegal transition and stop fail-closed.",
            ],
        }
    finally:
        await redis_client.aclose()
        if old_binding is None:
            os.environ.pop(
                "HFA_CANONICAL_TASK_ADMIT_BINDING",
                None,
            )
        else:
            os.environ[
                "HFA_CANONICAL_TASK_ADMIT_BINDING"
            ] = old_binding


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
            "SPRINT83_5_ACCEPTANCE "
            f"status={report['status']} "
            "user_facing_http_submit_supported="
            f"{str(report['user_facing_http_submit_supported']).lower()} "
            f"report={output}"
        )
    return 0 if report["status"] != "FAIL" else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run Sprint 83.5 user-facing "
            "runtime end-to-end acceptance"
        )
    )
    parser.add_argument(
        "--redis-url",
        default="redis://127.0.0.1:6389/0",
    )
    parser.add_argument(
        "--acceptance-id",
        default="s83-5",
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
