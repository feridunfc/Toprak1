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
from hfa_control.product_profile import (
    ProductMode,
    TenantIdentityBoundary,
)
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
    / "sprint83_7_trusted_gateway_product_alpha.json"
)

PRIVATE_FAILURE = (
    "redis://admin:super-secret@private-host:6379 "
    "C:\\private\\provider\\trace.py "
    'provider_body={"api_key":"sk-live-secret"}'
)

PUBLIC_FAILURE = {
    "code": "EXECUTOR_FAILED",
    "message": "Task execution failed.",
    "retryable": False,
}


class ControlledAlphaExecutor:
    product_executor_capability = "executor:deterministic"

    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []
        self.success_started = asyncio.Event()
        self.success_release = asyncio.Event()
        self.failure_started = asyncio.Event()
        self.failure_release = asyncio.Event()

    async def execute(self, event):
        payload = dict(getattr(event, "payload", {}) or {})
        mode = str(payload.get("mode") or "success")
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
                "mode": mode,
            }
        )

        if mode == "success":
            self.success_started.set()
            await self.success_release.wait()
            return SimpleNamespace(
                status="done",
                payload={
                    "output_text":
                        "SPRINT83_7_ALPHA_SUCCESS",
                },
                error="",
            )

        if mode == "failure":
            self.failure_started.set()
            await self.failure_release.wait()
            return SimpleNamespace(
                status="failed",
                payload={
                    "provider_body": PRIVATE_FAILURE,
                    "internal_path":
                        "C:\\private\\provider\\trace.py",
                },
                error=PRIVATE_FAILURE,
            )

        return SimpleNamespace(
            status="done",
            payload={
                "output_text":
                    "SPRINT83_7_DUPLICATE_POST_RUN",
                "mode": mode,
            },
            error="",
        )


class ReplacementExecutor:
    product_executor_capability = "executor:deterministic"

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(self, event):
        self.calls.append(
            str(getattr(event, "run_id", "") or "")
        )
        return SimpleNamespace(
            status="done",
            payload={"unexpected": True},
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
    timeout_seconds: float = 25.0,
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


async def _event_set(event: asyncio.Event) -> bool:
    return event.is_set()


def _control_config(
    *,
    acceptance_id: str,
    instance_suffix: str,
) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id=(
            f"cp-s83-7-{acceptance_id}-{instance_suffix}"
        ),
        region="sprint83-7-acceptance",
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
        product_mode=ProductMode.SINGLE_TASK_ALPHA.value,
        tenant_identity_boundary=(
            TenantIdentityBoundary
            .TRUSTED_GATEWAY_HEADER
            .value
        ),
    )


def _worker_config(
    redis_client,
    *,
    worker_id: str,
    worker_group: str,
    executor: object,
) -> dict[str, Any]:
    return {
        "product_mode":
            ProductMode.SINGLE_TASK_ALPHA.value,
        "production": True,
        "worker_id": worker_id,
        "worker_group": worker_group,
        "region": "sprint83-7-acceptance",
        "shards": [0],
        "capacity": 1,
        "version": "83.7-acceptance",
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


def _build_client(
    control: ControlPlaneService,
    redis_client,
    *,
    suffix: str,
) -> httpx.AsyncClient:
    app = FastAPI()
    app.include_router(router)
    app.state.cp = control
    app.state.redis = redis_client
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url=f"http://sprint83-7-{suffix}.test",
    )


async def _close_control(
    control: ControlPlaneService,
    client: httpx.AsyncClient,
) -> None:
    await client.aclose()
    await control._redis_monitor.close()
    await control.close()


async def _product_ready(
    client: httpx.AsyncClient,
):
    response = await client.get(
        "/control/v1/product/readiness"
    )
    if response.status_code != 200:
        return None
    payload = response.json()
    return payload if payload.get("ready") is True else None


async def _run_view(
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
    return response.json()


async def _run_status(
    client: httpx.AsyncClient,
    *,
    run_id: str,
    tenant_id: str,
    expected_status: str,
):
    payload = await _run_view(
        client,
        run_id=run_id,
        tenant_id=tenant_id,
    )
    if payload is None:
        return None
    if payload.get("status") != expected_status:
        return None
    return payload


async def _complete_terminal_view(
    client: httpx.AsyncClient,
    *,
    run_id: str,
    tenant_id: str,
    expected_status: str,
    expected_task_state: str,
):
    # Require terminal authority and complete public result evidence.
    payload = await _run_view(
        client,
        run_id=run_id,
        tenant_id=tenant_id,
    )
    if payload is None:
        return None
    if payload.get("status") != expected_status:
        return None
    if payload.get("terminal") is not True:
        return None
    if (
        payload.get("completeness")
        != "TERMINAL_WITH_RESULT"
    ):
        return None
    if (
        payload.get("task_output_status")
        != "AVAILABLE"
    ):
        return None
    if (
        payload.get("task_state")
        != expected_task_state
    ):
        return None
    return payload


async def _task_state(
    redis_client,
    *,
    task_id: str,
    expected_state: str,
):
    state = _decode(
        await redis_client.get(
            DagRedisKey.task_state(task_id)
        )
    ).strip()
    return state if state == expected_state else None


def _stable_terminal_projection(
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in (
            "schema_version",
            "run_id",
            "status",
            "terminal",
            "outcome",
            "result",
            "error",
            "submitted_at",
            "started_at",
            "finished_at",
            "updated_at",
            "completeness",
            "completeness_reason",
            "internal_state",
            "task_counts",
            "issues",
            "task_output_status",
            "task_id",
            "task_state",
            "task_output",
            "task_output_issues",
        )
    }


async def _run_evidence_digest(
    redis_client,
    *,
    run_task_pairs: list[tuple[str, str]],
) -> str:
    keys: list[str] = []
    for run_id, task_id in run_task_pairs:
        keys.extend(
            [
                RedisKey.run_state(run_id),
                RedisKey.run_meta(run_id),
                RedisKey.run_result(run_id),
                DagRedisKey.run_tasks(run_id),
                DagRedisKey.task_state(task_id),
                DagRedisKey.task_meta(task_id),
                DagRedisKey.task_output(task_id),
            ]
        )

    digest = hashlib.sha256()
    for key in sorted(set(keys)):
        dumped = await redis_client.dump(key)
        digest.update(key.encode("utf-8"))
        digest.update(b"\0")
        digest.update(dumped or b"<missing>")
        digest.update(b"\0")
    return digest.hexdigest()


async def _all_key_names(redis_client) -> tuple[str, ...]:
    result = []
    async for key in redis_client.scan_iter(match="*"):
        result.append(_decode(key))
    return tuple(sorted(result))


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
    result = []
    for _entry_id, fields in await redis_client.xrange(
        RedisKey.stream_results()
    ):
        decoded = _decode_mapping(fields)
        if decoded.get("run_id") == run_id:
            result.append(decoded)
    return result


async def _terminal_pair(
    client: httpx.AsyncClient,
    *,
    tenant_id: str,
    run_ids: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    result = {}
    for run_id in run_ids:
        result[run_id] = await _wait_until(
            lambda run_id=run_id: _run_view(
                client,
                run_id=run_id,
                tenant_id=tenant_id,
            ),
            description=f"terminal read for {run_id}",
        )
        if result[run_id].get("terminal") is not True:
            raise RuntimeError(
                f"RUN is not terminal: {run_id} "
                f"{result[run_id]}"
            )
    return result


async def _product_alpha_scenario(
    redis_client,
    *,
    acceptance_id: str,
) -> dict[str, Any]:
    tenant_id = f"tenant{acceptance_id.replace('-', '')}"
    other_tenant_id = f"{tenant_id}other"
    worker_id = f"worker-s83-7-{acceptance_id}"
    worker_group = f"group-s83-7-{acceptance_id}"

    executor = ControlledAlphaExecutor()
    control = ControlPlaneService(
        redis_client,
        _control_config(
            acceptance_id=acceptance_id,
            instance_suffix="initial",
        ),
    )
    control._run_submission = RunSubmissionCoordinator(
        admission_controller=control._admitter,
        dag_lua=control._scheduler.composition.dag_lua,
        uuid_factory=DeterministicUUIDFactory(
            *[
                _uuid4_from_material(
                    f"{acceptance_id}:{label}"
                )
                for label in (
                    "success-run",
                    "success-task",
                    "failure-run",
                    "failure-task",
                    "duplicate-a-run",
                    "duplicate-a-task",
                    "duplicate-b-run",
                    "duplicate-b-task",
                )
            ]
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
    client = _build_client(
        control,
        redis_client,
        suffix="initial",
    )

    replacement: WorkerService | None = None
    replacement_executor: ReplacementExecutor | None = None
    control_restarted = False
    try:
        await control.start()
        await worker.start()

        initial_readiness = await _wait_until(
            lambda: _product_ready(client),
            description="initial product readiness",
        )
        capabilities_response = await client.get(
            "/control/v1/product/capabilities"
        )
        capabilities = capabilities_response.json()

        await control._scheduler.stop()
        control._sched_started = False

        success_submit = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": tenant_id},
            json={
                "run_shape": "SINGLE_TASK",
                "payload": {
                    "mode": "success",
                    "prompt":
                        "Sprint 83.7 controlled success",
                },
                "agent_type": "sprint83-7-acceptance",
                "priority": 5,
                "estimated_cost_cents": 0,
                "preferred_region": "",
                "preferred_placement": "LEAST_LOADED",
                "required_capabilities": [],
                "trace_parent": "",
                "trace_state": "",
            },
        )
        success_submit_body = success_submit.json()
        success_run_id = str(
            success_submit_body.get("run_id") or ""
        )
        success_task_id = str(
            success_submit_body.get("task_id") or ""
        )

        queued_view = await _run_status(
            client,
            run_id=success_run_id,
            tenant_id=tenant_id,
            expected_status="QUEUED",
        )
        if queued_view is None:
            raise RuntimeError(
                "Controlled QUEUED view was not visible"
            )

        token = int(
            getattr(control._leader, "fencing_token", 0)
            or 0
        )
        if token <= 0:
            raise RuntimeError(
                "Leader fencing token unavailable"
            )
        await control._scheduler.start(
            scheduler_epoch=str(token)
        )
        control._sched_started = True

        await _wait_until(
            lambda: _event_set(executor.success_started),
            description="success executor start",
        )
        success_task_running = await _wait_until(
            lambda: _task_state(
                redis_client,
                task_id=success_task_id,
                expected_state="running",
            ),
            description="canonical TASK running state",
        )
        success_public_during_execution = await _run_view(
            client,
            run_id=success_run_id,
            tenant_id=tenant_id,
        )
        if (
            success_public_during_execution is None
            or success_public_during_execution.get("status")
            != "QUEUED"
        ):
            raise RuntimeError(
                "RUN authority must remain QUEUED while "
                "canonical TASK execution is running"
            )

        executor.success_release.set()
        success_terminal = await _wait_until(
            lambda: _complete_terminal_view(
                client,
                run_id=success_run_id,
                tenant_id=tenant_id,
                expected_status="COMPLETED",
                expected_task_state="done",
            ),
            description=(
                "complete HTTP COMPLETED success view"
            ),
        )

        failure_submit = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": tenant_id},
            json={
                "run_shape": "SINGLE_TASK",
                "payload": {
                    "mode": "failure",
                    "prompt":
                        "Sprint 83.7 controlled failure",
                },
                "agent_type": "sprint83-7-acceptance",
                "priority": 5,
                "estimated_cost_cents": 0,
                "preferred_region": "",
                "preferred_placement": "LEAST_LOADED",
                "required_capabilities": [],
                "trace_parent": "",
                "trace_state": "",
            },
        )
        failure_submit_body = failure_submit.json()
        failure_run_id = str(
            failure_submit_body.get("run_id") or ""
        )
        failure_task_id = str(
            failure_submit_body.get("task_id") or ""
        )

        await _wait_until(
            lambda: _event_set(executor.failure_started),
            description="failure executor start",
        )
        failure_task_running = await _wait_until(
            lambda: _task_state(
                redis_client,
                task_id=failure_task_id,
                expected_state="running",
            ),
            description="canonical failing TASK running state",
        )
        failure_public_during_execution = await _run_view(
            client,
            run_id=failure_run_id,
            tenant_id=tenant_id,
        )
        if (
            failure_public_during_execution is None
            or failure_public_during_execution.get("status")
            != "QUEUED"
        ):
            raise RuntimeError(
                "RUN authority must remain QUEUED while "
                "canonical failing TASK execution is running"
            )
        executor.failure_release.set()
        failure_terminal = await _wait_until(
            lambda: _complete_terminal_view(
                client,
                run_id=failure_run_id,
                tenant_id=tenant_id,
                expected_status="FAILED",
                expected_task_state="failed",
            ),
            description=(
                "complete HTTP FAILED view"
            ),
        )

        raw_failure_output = _decode(
            await redis_client.get(
                DagRedisKey.task_output(failure_task_id)
            )
        )
        legacy_failure_response = await client.get(
            f"/control/v1/runs/{failure_run_id}/result",
            headers={"X-Tenant-ID": tenant_id},
        )
        legacy_failure = legacy_failure_response.json()

        keys_before_unsupported = await _all_key_names(
            redis_client
        )
        unsupported = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": tenant_id},
            json={
                "run_shape": "MULTI_TASK",
                "payload": {"tasks": [1, 2]},
            },
        )
        keys_after_unsupported = await _all_key_names(
            redis_client
        )

        duplicate_body = {
            "run_shape": "SINGLE_TASK",
            "payload": {
                "mode": "duplicate",
                "prompt": "same duplicate request",
            },
            "agent_type": "sprint83-7-acceptance",
            "priority": 5,
            "estimated_cost_cents": 0,
            "preferred_region": "",
            "preferred_placement": "LEAST_LOADED",
            "required_capabilities": [],
            "trace_parent": "",
            "trace_state": "",
        }
        duplicate_a = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": tenant_id},
            json=duplicate_body,
        )
        duplicate_b = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": tenant_id},
            json=duplicate_body,
        )
        duplicate_a_body = duplicate_a.json()
        duplicate_b_body = duplicate_b.json()
        duplicate_a_run = str(
            duplicate_a_body.get("run_id") or ""
        )
        duplicate_b_run = str(
            duplicate_b_body.get("run_id") or ""
        )
        duplicate_a_task = str(
            duplicate_a_body.get("task_id") or ""
        )
        duplicate_b_task = str(
            duplicate_b_body.get("task_id") or ""
        )

        duplicate_terminals = {}
        for run_id in (
            duplicate_a_run,
            duplicate_b_run,
        ):
            duplicate_terminals[run_id] = (
                await _wait_until(
                    lambda run_id=run_id:
                        _complete_terminal_view(
                            client,
                            run_id=run_id,
                            tenant_id=tenant_id,
                            expected_status="COMPLETED",
                            expected_task_state="done",
                        ),
                    description=(
                        "complete duplicate POST terminal "
                        f"{run_id}"
                    ),
                )
            )

        tenant_isolation = await client.get(
            f"/control/v1/runs/{success_run_id}",
            headers={"X-Tenant-ID": other_tenant_id},
        )
        unknown_run_id = (
            f"run-{tenant_id}-"
            f"{_uuid4_from_material(f'{acceptance_id}:unknown')}"
        )
        unknown = await client.get(
            f"/control/v1/runs/{unknown_run_id}",
            headers={"X-Tenant-ID": tenant_id},
        )

        run_task_pairs = [
            (success_run_id, success_task_id),
            (failure_run_id, failure_task_id),
            (duplicate_a_run, duplicate_a_task),
            (duplicate_b_run, duplicate_b_task),
        ]
        digest_before_gets = await _run_evidence_digest(
            redis_client,
            run_task_pairs=run_task_pairs,
        )
        duplicate_get_one = await _run_view(
            client,
            run_id=success_run_id,
            tenant_id=tenant_id,
        )
        duplicate_get_two = await _run_view(
            client,
            run_id=success_run_id,
            tenant_id=tenant_id,
        )
        digest_after_gets = await _run_evidence_digest(
            redis_client,
            run_task_pairs=run_task_pairs,
        )

        stable_before_restart = {
            success_run_id:
                _stable_terminal_projection(
                    success_terminal
                ),
            failure_run_id:
                _stable_terminal_projection(
                    failure_terminal
                ),
            duplicate_a_run:
                _stable_terminal_projection(
                    duplicate_terminals[
                        duplicate_a_run
                    ]
                ),
            duplicate_b_run:
                _stable_terminal_projection(
                    duplicate_terminals[
                        duplicate_b_run
                    ]
                ),
        }

        await _close_control(control, client)
        control_restarted = True

        control = ControlPlaneService(
            redis_client,
            _control_config(
                acceptance_id=acceptance_id,
                instance_suffix="restart",
            ),
        )
        client = _build_client(
            control,
            redis_client,
            suffix="restart",
        )
        await control.start()
        restart_readiness = await _wait_until(
            lambda: _product_ready(client),
            description="product readiness after control restart",
        )

        stable_after_control_restart = {}
        for run_id in stable_before_restart:
            payload = await _run_view(
                client,
                run_id=run_id,
                tenant_id=tenant_id,
            )
            if payload is None:
                raise RuntimeError(
                    f"RUN unreadable after control restart: {run_id}"
                )
            stable_after_control_restart[run_id] = (
                _stable_terminal_projection(payload)
            )

        await worker.close(drain_timeout=0)
        replacement_executor = ReplacementExecutor()
        replacement = WorkerService(
            redis_client,
            _worker_config(
                redis_client,
                worker_id=worker_id,
                worker_group=worker_group,
                executor=replacement_executor,
            ),
        )
        await replacement.start()
        worker_restart_readiness = await _wait_until(
            lambda: _product_ready(client),
            description="product readiness after worker restart",
        )
        await asyncio.sleep(0.5)

        stable_after_worker_restart = {}
        for run_id in stable_before_restart:
            payload = await _run_view(
                client,
                run_id=run_id,
                tenant_id=tenant_id,
            )
            if payload is None:
                raise RuntimeError(
                    f"RUN unreadable after worker restart: {run_id}"
                )
            stable_after_worker_restart[run_id] = (
                _stable_terminal_projection(payload)
            )

        success_events = await _result_events(
            redis_client,
            run_id=success_run_id,
        )
        failure_events = await _result_events(
            redis_client,
            run_id=failure_run_id,
        )
        pending_count = await _pending_count(
            redis_client,
            stream=RedisKey.stream_shard(0),
        )

        public_failure_json = json.dumps(
            failure_terminal,
            sort_keys=True,
        )
        passed = all(
            (
                initial_readiness.get("ready") is True,
                initial_readiness.get("product_mode")
                == "SINGLE_TASK_ALPHA",
                initial_readiness.get(
                    "tenant_identity_boundary"
                ) == "TRUSTED_GATEWAY_HEADER",
                initial_readiness.get(
                    "compatible_worker_count"
                ) == 1,
                capabilities_response.status_code == 200,
                capabilities.get(
                    "supported_run_shapes"
                ) == ["SINGLE_TASK"],
                capabilities.get(
                    "multi_task_result_supported"
                ) is False,
                capabilities.get("cancel_supported")
                is False,
                capabilities.get("retry_supported")
                is False,
                capabilities.get(
                    "submission_idempotency_supported"
                ) is False,
                capabilities.get("production_ready")
                is False,
                success_submit.status_code == 202,
                success_submit_body.get("status")
                == "ACCEPTED",
                queued_view.get("status") == "QUEUED",
                success_task_running == "running",
                success_public_during_execution.get("status")
                == "QUEUED",
                success_terminal.get("status")
                == "COMPLETED",
                success_terminal.get("outcome")
                == "SUCCESS",
                success_terminal.get(
                    "task_output_status"
                ) == "AVAILABLE",
                success_terminal.get("task_state")
                == "done",
                success_terminal.get("task_output")
                == {
                    "output_text":
                        "SPRINT83_7_ALPHA_SUCCESS"
                },
                failure_submit.status_code == 202,
                failure_task_running == "running",
                failure_public_during_execution.get("status")
                == "QUEUED",
                failure_terminal.get("status")
                == "FAILED",
                failure_terminal.get("outcome")
                == "FAILURE",
                failure_terminal.get("error")
                == PUBLIC_FAILURE,
                failure_terminal.get(
                    "task_output_status"
                ) == "AVAILABLE",
                failure_terminal.get("task_state")
                == "failed",
                failure_terminal.get("task_output")
                == PUBLIC_FAILURE,
                PRIVATE_FAILURE not in raw_failure_output,
                PRIVATE_FAILURE not in public_failure_json,
                "sk-live-secret"
                not in public_failure_json,
                legacy_failure_response.status_code
                == 200,
                legacy_failure.get("error")
                == "Task execution failed.",
                PRIVATE_FAILURE not in json.dumps(
                    legacy_failure,
                    sort_keys=True,
                ),
                unsupported.status_code == 400,
                unsupported.json().get("failure_code")
                == "UNSUPPORTED_RUN_SHAPE",
                keys_before_unsupported
                == keys_after_unsupported,
                duplicate_a.status_code == 202,
                duplicate_b.status_code == 202,
                duplicate_a_run != duplicate_b_run,
                duplicate_a_run
                and duplicate_b_run,
                tenant_isolation.status_code == 403,
                unknown.status_code == 404,
                duplicate_get_one is not None,
                duplicate_get_two is not None,
                _stable_terminal_projection(
                    duplicate_get_one
                )
                == _stable_terminal_projection(
                    duplicate_get_two
                ),
                digest_before_gets == digest_after_gets,
                stable_after_control_restart
                == stable_before_restart,
                stable_after_worker_restart
                == stable_before_restart,
                restart_readiness.get("ready") is True,
                worker_restart_readiness.get("ready")
                is True,
                replacement_executor is not None,
                len(replacement_executor.calls) == 0,
                len(executor.calls) == 4,
                len(success_events) == 1,
                success_events[0].get("event_type")
                == "RunCompleted",
                len(failure_events) == 1,
                failure_events[0].get("event_type")
                == "RunFailed",
                pending_count == 0,
            )
        )

        return {
            "name":
                "trusted_gateway_single_task_product_alpha",
            "status": "PASS" if passed else "FAIL",
            "initial_product_readiness":
                initial_readiness,
            "restart_product_readiness":
                restart_readiness,
            "worker_restart_product_readiness":
                worker_restart_readiness,
            "capabilities": capabilities,
            "success": {
                "submit_status_code":
                    success_submit.status_code,
                "run_id": success_run_id,
                "task_id": success_task_id,
                "queued_status":
                    queued_view.get("status"),
                "task_running_state":
                    success_task_running,
                "public_run_status_while_task_running":
                    success_public_during_execution.get(
                        "status"
                    ),
                "terminal_status":
                    success_terminal.get("status"),
                "terminal_outcome":
                    success_terminal.get("outcome"),
                "task_output":
                    success_terminal.get("task_output"),
                "terminal_event_count":
                    len(success_events),
                "terminal_event_type": (
                    success_events[0].get(
                        "event_type",
                        "",
                    )
                    if success_events
                    else ""
                ),
            },
            "failure": {
                "submit_status_code":
                    failure_submit.status_code,
                "run_id": failure_run_id,
                "task_id": failure_task_id,
                "task_running_state":
                    failure_task_running,
                "public_run_status_while_task_running":
                    failure_public_during_execution.get(
                        "status"
                    ),
                "terminal_status":
                    failure_terminal.get("status"),
                "terminal_outcome":
                    failure_terminal.get("outcome"),
                "public_error":
                    failure_terminal.get("error"),
                "public_task_output":
                    failure_terminal.get("task_output"),
                "raw_failure_output_contains_private":
                    PRIVATE_FAILURE
                    in raw_failure_output,
                "public_response_contains_private":
                    PRIVATE_FAILURE
                    in public_failure_json,
                "legacy_error":
                    legacy_failure.get("error"),
                "terminal_event_count":
                    len(failure_events),
                "terminal_event_type": (
                    failure_events[0].get(
                        "event_type",
                        "",
                    )
                    if failure_events
                    else ""
                ),
            },
            "contract": {
                "unsupported_multi_task_status_code":
                    unsupported.status_code,
                "unsupported_multi_task_failure_code":
                    unsupported.json().get(
                        "failure_code"
                    ),
                "unsupported_multi_task_zero_new_keys":
                    keys_before_unsupported
                    == keys_after_unsupported,
                "duplicate_post_distinct_run":
                    duplicate_a_run
                    != duplicate_b_run,
                "duplicate_get_zero_writes":
                    digest_before_gets
                    == digest_after_gets,
                "tenant_isolation_status_code":
                    tenant_isolation.status_code,
                "unknown_run_status_code":
                    unknown.status_code,
            },
            "durability": {
                "control_service_restart_semantics_stable":
                    stable_after_control_restart
                    == stable_before_restart,
                "worker_service_restart_semantics_stable":
                    stable_after_worker_restart
                    == stable_before_restart,
                "replacement_executor_call_count": (
                    len(replacement_executor.calls)
                    if replacement_executor is not None
                    else -1
                ),
                "original_executor_call_count":
                    len(executor.calls),
                "pending_count": pending_count,
            },
            "architecture": {
                "production_scheduler_used": True,
                "production_worker_used": True,
                "real_redis_used": True,
                "real_asgi_application_used": True,
                "network_socket_http_used": False,
                "new_lifecycle_writer": False,
                "direct_lifecycle_write_used": False,
                "direct_dispatch_call_used": False,
                "automatic_repair_used": False,
                "Loop_Plane_dependencies": 0,
                "scheduler_paused_only_for_deterministic_queued_observation":
                    True,
                "public_run_running_state_authored":
                    False,
                "canonical_task_running_state_observed":
                    True,
            },
        }
    finally:
        executor.success_release.set()
        executor.failure_release.set()
        if replacement is not None:
            await replacement.close(drain_timeout=0)
        elif not getattr(worker, "_closed", False):
            await worker.close(drain_timeout=0)

        if not control_restarted:
            await _close_control(control, client)
        else:
            await _close_control(control, client)


async def build_acceptance_report(
    redis_url: str,
    *,
    acceptance_id: str = "s83-7",
    reset_test_db: bool = False,
) -> dict[str, Any]:
    acceptance_id = _validate_acceptance_id(
        acceptance_id
    )

    env_names = (
        "HFA_CANONICAL_TASK_ADMIT_BINDING",
        "HFA_PRODUCT_MODE",
        "HFA_TENANT_IDENTITY_BOUNDARY",
        "HFA_STRICT_CAS_MODE",
    )
    old_env = {
        name: os.environ.get(name)
        for name in env_names
    }
    os.environ[
        "HFA_CANONICAL_TASK_ADMIT_BINDING"
    ] = "1"
    os.environ["HFA_PRODUCT_MODE"] = (
        ProductMode.SINGLE_TASK_ALPHA.value
    )
    os.environ[
        "HFA_TENANT_IDENTITY_BOUNDARY"
    ] = (
        TenantIdentityBoundary
        .TRUSTED_GATEWAY_HEADER
        .value
    )
    os.environ["HFA_STRICT_CAS_MODE"] = "true"

    redis_client = redis_async.from_url(
        redis_url,
        decode_responses=False,
    )
    try:
        await redis_client.ping()
        if reset_test_db:
            await redis_client.flushdb()

        scenario = await _product_alpha_scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )
        passed = scenario["status"] == "PASS"
        return {
            "schema_version": 1,
            "sprint": "83.7",
            "source":
                "trusted_gateway_single_task_product_alpha",
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
            "real_asgi_application_used": True,
            "network_socket_http_used": False,
            "production_scheduler_used": passed,
            "production_worker_used": passed,
            "canonical_task_admit_used": passed,
            "run_finalization_supported": passed,
            "product_readiness_supported": passed,
            "capability_contract_supported": passed,
            "success_path": passed,
            "failure_path": passed,
            "canonical_task_running_observed": passed,
            "public_run_running_supported": False,
            "sanitized_failure_contract": passed,
            "control_service_restart_durability": passed,
            "worker_service_restart_durability": passed,
            "duplicate_get_zero_writes": passed,
            "duplicate_post_distinct_run": passed,
            "tenant_isolation": passed,
            "unsupported_multi_task_zero_writes": passed,
            "internal_product_alpha_ready": passed,
            "trusted_gateway_product_alpha_ready":
                passed,
            "product_alpha_ready": passed,
            "direct_public_multitenant_alpha_ready":
                False,
            "production_ready": False,
            "submission_idempotency_supported": False,
            "multi_task_support": False,
            "cancel_supported": False,
            "retry_supported": False,
            "external_executor_cutover": False,
            "automatic_repair": False,
            "archive_available": False,
            "new_lifecycle_writer": False,
            "direct_lifecycle_write_used": False,
            "direct_dispatch_call_used": False,
            "Loop_Plane_dependencies": 0,
            "production_cutover_authorized": False,
            "scenarios": [scenario],
            "limitations": [
                "Tenant identity is trusted from the gateway header; direct public tenant authentication is not provided.",
                "HTTP uses the real FastAPI/ASGI application through an in-process ASGI transport, not a network socket.",
                "Control and worker restart durability is proven by replacing service composition instances in-process against the same Redis database; separate operating-system process crash recovery is not claimed.",
                "The production scheduler is paused briefly only to make the initial QUEUED observation deterministic, then the real production scheduler performs dispatch.",
                "Canonical TASK state becomes running during execution, but RUN authority remains admitted/QUEUED until terminal RUN_TERMINATE; public RUNNING is not claimed and no new lifecycle writer is introduced.",
                "Terminal response comparison excludes naturally decreasing TTL fields and verifies the stable semantic projection.",
                "Result retention remains the fixed current 86400-second Redis contract; no durable archive exists.",
                "Duplicate POST intentionally creates a distinct RUN because submission idempotency is unsupported.",
                "Cancellation, retry, multi-task result aggregation, external executor cutover, automatic repair, and production cutover remain unsupported.",
            ],
        }
    finally:
        await redis_client.aclose()
        for name, old_value in old_env.items():
            if old_value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = old_value


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
            "SPRINT83_7_ACCEPTANCE "
            f"status={report['status']} "
            "trusted_gateway_product_alpha_ready="
            f"{str(report['trusted_gateway_product_alpha_ready']).lower()} "
            f"production_ready="
            f"{str(report['production_ready']).lower()} "
            f"report={output}"
        )
    return 0 if report["status"] != "FAIL" else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run Sprint 83.7 trusted-gateway "
            "single-task product alpha acceptance"
        )
    )
    parser.add_argument(
        "--redis-url",
        default="redis://127.0.0.1:6389/0",
    )
    parser.add_argument(
        "--acceptance-id",
        default="s83-7",
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
