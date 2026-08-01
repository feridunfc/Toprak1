from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

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
    if (
        candidate.exists()
        and str(candidate) not in sys.path
    ):
        sys.path.insert(0, str(candidate))

import httpx
import redis.asyncio as redis_async
from fastapi import FastAPI

from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.api.router import router
from hfa_control.models import ControlPlaneConfig
from hfa_control.product_profile import (
    ProductMode,
    TenantIdentityBoundary,
)
from hfa_control.run_submission import (
    RunSubmissionCoordinator,
)
from hfa_control.service import ControlPlaneService
from hfa_control.submission_idempotency import (
    submission_idempotency_redis_key,
)

from scripts.runtime_alpha_acceptance import (
    _decode,
    _validate_acceptance_id,
    write_report,
)


DEFAULT_OUTPUT = (
    ROOT
    / "local_out"
    / "sprint83"
    / "sprint83_8_idempotent_submission.json"
)


class BlockingAdmission:
    """Block exactly the first admission after reservation."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.calls: list[str] = []
        self.first_started = asyncio.Event()
        self.first_release = asyncio.Event()

    async def admit(self, request: Any):
        self.calls.append(str(request.run_id))
        if len(self.calls) == 1:
            self.first_started.set()
            await self.first_release.wait()
        return await self._delegate.admit(request)


class CountingDagLua:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.initialise_calls = 0
        self.task_admit_calls: list[str] = []

    async def initialise(self) -> None:
        self.initialise_calls += 1
        await self._delegate.initialise()

    async def task_admit(self, seed: Any):
        self.task_admit_calls.append(str(seed.task_id))
        return await self._delegate.task_admit(seed)


def _control_config(
    *,
    acceptance_id: str,
    suffix: str,
) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id=(
            f"cp-s83-8-{acceptance_id}-{suffix}"
        ),
        region="sprint83-8-acceptance",
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
        product_mode=(
            ProductMode.SINGLE_TASK_ALPHA.value
        ),
        tenant_identity_boundary=(
            TenantIdentityBoundary
            .TRUSTED_GATEWAY_HEADER
            .value
        ),
    )


def _client(
    control: ControlPlaneService,
    redis_client: Any,
    *,
    suffix: str,
) -> httpx.AsyncClient:
    app = FastAPI()
    app.include_router(router)
    app.state.cp = control
    app.state.redis = redis_client
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url=(
            f"http://sprint83-8-{suffix}.test"
        ),
    )


async def _keyspace_digest(
    redis_client: Any,
) -> str:
    keys: list[str] = []
    async for raw_key in redis_client.scan_iter(
        match="*"
    ):
        keys.append(_decode(raw_key))

    digest = hashlib.sha256()
    for key in sorted(keys):
        dumped = await redis_client.dump(key)
        kind = _decode(await redis_client.type(key))
        digest.update(key.encode("utf-8"))
        digest.update(b"\0")
        digest.update(kind.encode("utf-8"))
        digest.update(b"\0")
        digest.update(dumped or b"<missing>")
        digest.update(b"\0")
    return digest.hexdigest()


async def _key_names(
    redis_client: Any,
) -> tuple[str, ...]:
    values: list[str] = []
    async for raw_key in redis_client.scan_iter(
        match="*"
    ):
        values.append(_decode(raw_key))
    return tuple(sorted(values))


def _submission_body(
    *,
    prompt: str,
    trace_parent: str = "",
    trace_state: str = "",
) -> dict[str, Any]:
    return {
        "run_shape": "SINGLE_TASK",
        "payload": {
            "mode": "idempotency",
            "prompt": prompt,
        },
        "agent_type": "sprint83-8-acceptance",
        "priority": 5,
        "estimated_cost_cents": 0,
        "preferred_region": "",
        "preferred_placement": "LEAST_LOADED",
        "required_capabilities": [],
        "trace_parent": trace_parent,
        "trace_state": trace_state,
    }


async def _scenario(
    redis_client: Any,
    *,
    acceptance_id: str,
) -> dict[str, Any]:
    tenant_id = (
        f"tenant{acceptance_id.replace('-', '')}"
    )
    other_tenant_id = f"{tenant_id}other"
    corrupt_tenant_id = f"{tenant_id}corrupt"
    idempotency_key = (
        f"{acceptance_id}-canonical-request"
    )
    body = _submission_body(
        prompt="canonical idempotent request",
        trace_parent="trace-first",
        trace_state="state-first",
    )

    control = ControlPlaneService(
        redis_client,
        _control_config(
            acceptance_id=acceptance_id,
            suffix="initial",
        ),
    )
    # No background service loops are required for admission/TASK_ADMIT.
    # Disable audit calls because ControlPlaneService.start() is
    # intentionally not invoked in this deterministic submission proof.
    control._admitter._audit = None

    blocking_admission = BlockingAdmission(
        control._admitter
    )
    counting_dag = CountingDagLua(
        control._scheduler.composition.dag_lua
    )
    control._run_submission = (
        RunSubmissionCoordinator(
            admission_controller=blocking_admission,
            dag_lua=counting_dag,
            idempotency_store=(
                control._submission_idempotency
            ),
            require_idempotency_key=True,
        )
    )
    client = _client(
        control,
        redis_client,
        suffix="initial",
    )

    restarted_client: httpx.AsyncClient | None = None
    try:
        capabilities_response = await client.get(
            "/control/v1/product/capabilities"
        )
        capabilities = capabilities_response.json()

        first_task = asyncio.create_task(
            client.post(
                "/control/v1/runs",
                headers={
                    "X-Tenant-ID": tenant_id,
                    "Idempotency-Key":
                        idempotency_key,
                },
                json=body,
            )
        )
        await asyncio.wait_for(
            blocking_admission.first_started.wait(),
            timeout=10,
        )

        in_progress = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
                "Idempotency-Key":
                    idempotency_key,
            },
            json=body,
        )
        blocking_admission.first_release.set()
        first = await asyncio.wait_for(
            first_task,
            timeout=20,
        )

        first_body = first.json()
        run_id = str(
            first_body.get("run_id") or ""
        )
        task_id = str(
            first_body.get("task_id") or ""
        )

        idempotency_redis_key = (
            submission_idempotency_redis_key(
                tenant_id,
                idempotency_key,
            )
        )
        idempotency_record = {
            _decode(key): _decode(value)
            for key, value in (
                await redis_client.hgetall(
                    idempotency_redis_key
                )
            ).items()
        }
        idempotency_ttl = await redis_client.ttl(
            idempotency_redis_key
        )

        before_replay = await _keyspace_digest(
            redis_client
        )
        admission_calls_before_replay = len(
            blocking_admission.calls
        )
        task_calls_before_replay = len(
            counting_dag.task_admit_calls
        )

        replay = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
                "Idempotency-Key":
                    idempotency_key,
            },
            json=_submission_body(
                prompt=(
                    "canonical idempotent request"
                ),
                trace_parent="trace-retry",
                trace_state="state-retry",
            ),
        )
        after_replay = await _keyspace_digest(
            redis_client
        )

        before_conflict = after_replay
        conflict = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
                "Idempotency-Key":
                    idempotency_key,
            },
            json=_submission_body(
                prompt="different request payload"
            ),
        )
        after_conflict = await _keyspace_digest(
            redis_client
        )

        before_validation_failures = (
            after_conflict
        )
        missing_key = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
            },
            json=body,
        )
        invalid_key = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
                "Idempotency-Key": "x" * 129,
            },
            json=body,
        )
        after_validation_failures = (
            await _keyspace_digest(redis_client)
        )

        other = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": other_tenant_id,
                "Idempotency-Key":
                    idempotency_key,
            },
            json=body,
        )
        other_body = other.json()
        other_idempotency_key = (
            submission_idempotency_redis_key(
                other_tenant_id,
                idempotency_key,
            )
        )

        corrupt_key_value = (
            f"{acceptance_id}-corrupt"
        )
        corrupt_redis_key = (
            submission_idempotency_redis_key(
                corrupt_tenant_id,
                corrupt_key_value,
            )
        )
        await redis_client.set(
            corrupt_redis_key,
            "wrong-type",
            ex=RedisTTL.SUBMISSION_IDEMPOTENCY,
        )
        before_corrupt = await _keyspace_digest(
            redis_client
        )
        corrupt = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID":
                    corrupt_tenant_id,
                "Idempotency-Key":
                    corrupt_key_value,
            },
            json=body,
        )
        after_corrupt = await _keyspace_digest(
            redis_client
        )

        await client.aclose()

        restarted = ControlPlaneService(
            redis_client,
            _control_config(
                acceptance_id=acceptance_id,
                suffix="restart",
            ),
        )
        restarted._admitter._audit = None
        restarted_client = _client(
            restarted,
            redis_client,
            suffix="restart",
        )
        before_restart_replay = (
            await _keyspace_digest(redis_client)
        )
        restart_replay = await restarted_client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
                "Idempotency-Key":
                    idempotency_key,
            },
            json=body,
        )
        after_restart_replay = (
            await _keyspace_digest(redis_client)
        )

        replay_body = replay.json()
        conflict_body = conflict.json()
        in_progress_body = in_progress.json()
        restart_replay_body = (
            restart_replay.json()
        )

        key_names = await _key_names(redis_client)
        run_state_keys = [
            key
            for key in key_names
            if key.startswith("hfa:run:state:")
        ]

        passed = all(
            (
                capabilities_response.status_code
                == 200,
                capabilities.get(
                    "submission_idempotency_supported"
                ) is True,
                capabilities.get("production_ready")
                is False,
                in_progress.status_code == 409,
                in_progress_body.get(
                    "failure_code"
                ) == "IDEMPOTENCY_IN_PROGRESS",
                first.status_code == 202,
                first_body.get("status")
                == "ACCEPTED",
                first_body.get(
                    "idempotent_replay"
                ) is False,
                bool(run_id),
                bool(task_id),
                idempotency_record.get("state")
                == "FINAL",
                idempotency_record.get("tenant_id")
                == tenant_id,
                idempotency_record.get("run_id")
                == run_id,
                idempotency_record.get("task_id")
                == task_id,
                0 < idempotency_ttl
                <= RedisTTL.SUBMISSION_IDEMPOTENCY,
                idempotency_key
                not in idempotency_redis_key,
                replay.status_code == 200,
                replay_body.get("status")
                == "ACCEPTED",
                replay_body.get(
                    "idempotent_replay"
                ) is True,
                replay_body.get("run_id")
                == run_id,
                replay_body.get("task_id")
                == task_id,
                before_replay == after_replay,
                len(blocking_admission.calls)
                == admission_calls_before_replay
                + 1,
                len(counting_dag.task_admit_calls)
                == task_calls_before_replay
                + 1,
                conflict.status_code == 409,
                conflict_body.get(
                    "failure_code"
                ) == "IDEMPOTENCY_KEY_REUSED",
                conflict_body.get("run_id")
                == run_id,
                conflict_body.get("task_id")
                == task_id,
                before_conflict == after_conflict,
                missing_key.status_code == 400,
                missing_key.json().get(
                    "failure_code"
                ) == "INVALID_IDEMPOTENCY_KEY",
                invalid_key.status_code == 400,
                invalid_key.json().get(
                    "failure_code"
                ) == "INVALID_IDEMPOTENCY_KEY",
                before_validation_failures
                == after_validation_failures,
                other.status_code == 202,
                other_body.get("status")
                == "ACCEPTED",
                other_body.get("run_id") != run_id,
                other_idempotency_key
                != idempotency_redis_key,
                corrupt.status_code == 503,
                corrupt.json().get(
                    "failure_code"
                ) == "IDEMPOTENCY_STORE_FAILED",
                before_corrupt == after_corrupt,
                restart_replay.status_code == 200,
                restart_replay_body.get("run_id")
                == run_id,
                restart_replay_body.get("task_id")
                == task_id,
                restart_replay_body.get(
                    "idempotent_replay"
                ) is True,
                before_restart_replay
                == after_restart_replay,
                len(run_state_keys) == 2,
            )
        )

        return {
            "name":
                "trusted_gateway_idempotent_submission",
            "status": "PASS" if passed else "FAIL",
            "capabilities": capabilities,
            "first_submission": {
                "status_code": first.status_code,
                "run_id": run_id,
                "task_id": task_id,
                "idempotent_replay":
                    first_body.get(
                        "idempotent_replay"
                    ),
            },
            "in_progress_replay": {
                "status_code":
                    in_progress.status_code,
                "failure_code":
                    in_progress_body.get(
                        "failure_code"
                    ),
                "run_id":
                    in_progress_body.get("run_id"),
                "task_id":
                    in_progress_body.get("task_id"),
            },
            "final_replay": {
                "status_code": replay.status_code,
                "idempotent_replay":
                    replay_body.get(
                        "idempotent_replay"
                    ),
                "same_run_id":
                    replay_body.get("run_id")
                    == run_id,
                "same_task_id":
                    replay_body.get("task_id")
                    == task_id,
                "zero_keyspace_mutation":
                    before_replay == after_replay,
            },
            "payload_conflict": {
                "status_code":
                    conflict.status_code,
                "failure_code":
                    conflict_body.get(
                        "failure_code"
                    ),
                "zero_keyspace_mutation":
                    before_conflict
                    == after_conflict,
            },
            "tenant_namespace": {
                "same_visible_key":
                    idempotency_key,
                "distinct_redis_keys":
                    other_idempotency_key
                    != idempotency_redis_key,
                "distinct_run_ids":
                    other_body.get("run_id")
                    != run_id,
            },
            "validation": {
                "missing_key_status_code":
                    missing_key.status_code,
                "invalid_key_status_code":
                    invalid_key.status_code,
                "zero_keyspace_mutation":
                    before_validation_failures
                    == after_validation_failures,
            },
            "corrupt_evidence": {
                "status_code":
                    corrupt.status_code,
                "failure_code":
                    corrupt.json().get(
                        "failure_code"
                    ),
                "zero_keyspace_mutation":
                    before_corrupt == after_corrupt,
            },
            "durability": {
                "composition_restart_replay_status":
                    restart_replay.status_code,
                "same_run_id":
                    restart_replay_body.get(
                        "run_id"
                    ) == run_id,
                "same_task_id":
                    restart_replay_body.get(
                        "task_id"
                    ) == task_id,
                "zero_keyspace_mutation":
                    before_restart_replay
                    == after_restart_replay,
            },
            "retention": {
                "ttl_seconds": idempotency_ttl,
                "configured_seconds":
                    RedisTTL.SUBMISSION_IDEMPOTENCY,
                "aligned_with_run_result":
                    RedisTTL.SUBMISSION_IDEMPOTENCY
                    == RedisTTL.RUN_RESULT,
            },
            "architecture": {
                "real_redis_used": True,
                "real_asgi_application_used": True,
                "production_control_composition_used":
                    True,
                "existing_run_admission_used": True,
                "existing_task_admit_used": True,
                "new_lifecycle_writer": False,
                "automatic_repair_used": False,
                "stale_owner_takeover_used": False,
                "direct_dispatch_used": False,
                "background_service_loops_started":
                    False,
            },
        }
    finally:
        blocking_admission.first_release.set()
        if restarted_client is not None:
            await restarted_client.aclose()
        elif not client.is_closed:
            await client.aclose()


async def build_acceptance_report(
    redis_url: str,
    *,
    acceptance_id: str = "s83-8",
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

        scenario = await _scenario(
            redis_client,
            acceptance_id=acceptance_id,
        )
        passed = scenario["status"] == "PASS"
        return {
            "schema_version": 1,
            "sprint": "83.8",
            "source":
                "trusted_gateway_idempotent_submission",
            "status": (
                "PASS_WITH_LIMITATIONS"
                if passed
                else "FAIL"
            ),
            "acceptance_id": acceptance_id,
            "real_redis_used": True,
            "real_asgi_application_used": True,
            "production_control_composition_used":
                passed,
            "submission_idempotency_supported":
                passed,
            "same_key_same_request_replay":
                passed,
            "same_key_different_request_conflict":
                passed,
            "concurrent_in_progress_fail_closed":
                passed,
            "tenant_scoped_namespace": passed,
            "missing_key_fail_closed": passed,
            "invalid_key_fail_closed": passed,
            "corrupt_evidence_fail_closed": passed,
            "composition_restart_durability":
                passed,
            "zero_replay_lifecycle_writes": passed,
            "raw_idempotency_key_persisted": False,
            "retention_seconds":
                RedisTTL.SUBMISSION_IDEMPOTENCY,
            "multi_task_support": False,
            "cancel_supported": False,
            "retry_supported": False,
            "automatic_repair": False,
            "stale_owner_takeover": False,
            "external_executor_cutover": False,
            "production_ready": False,
            "production_cutover_authorized": False,
            "new_lifecycle_writer": False,
            "scenarios": [scenario],
            "limitations": [
                (
                    "Tenant identity remains trusted from "
                    "the gateway header; direct public "
                    "authentication is not provided."
                ),
                (
                    "The acceptance uses the production "
                    "control composition and real ASGI "
                    "surface without starting background "
                    "scheduler, recovery, registry, or "
                    "leadership loops."
                ),
                (
                    "A control composition replacement "
                    "against the same Redis database is "
                    "proven; an operating-system process "
                    "crash is not simulated here."
                ),
                (
                    "IN_PROGRESS reservations fail closed "
                    "until expiry; stale-owner takeover and "
                    "automatic repair are not supported."
                ),
                (
                    "Cancellation, retry/requeue, multi-task "
                    "aggregation, external executor cutover, "
                    "archive, and production cutover remain "
                    "unsupported."
                ),
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
            "SPRINT83_8_ACCEPTANCE "
            f"status={report['status']} "
            "submission_idempotency_supported="
            f"{str(report['submission_idempotency_supported']).lower()} "
            "production_ready="
            f"{str(report['production_ready']).lower()}"
        )
    return 0 if report["status"] == (
        "PASS_WITH_LIMITATIONS"
    ) else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sprint 83.8 trusted-gateway "
            "idempotent submission acceptance"
        )
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get(
            "REDIS_URL",
            "redis://127.0.0.1:6389/0",
        ),
    )
    parser.add_argument(
        "--acceptance-id",
        default="s83-8",
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
    return parser


def main() -> int:
    return asyncio.run(
        _async_main(_parser().parse_args())
    )


if __name__ == "__main__":
    raise SystemExit(main())
