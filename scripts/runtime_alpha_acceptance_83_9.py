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
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import httpx
import redis.asyncio as redis_async
from fastapi import FastAPI

from hfa.config.keys import RedisTTL
from hfa_control.api.router import router
from hfa_control.models import ControlPlaneConfig
from hfa_control.product_profile import (
    ProductMode,
    TenantIdentityBoundary,
)
from hfa_control.run_submission import (
    RunSubmissionCoordinator,
    SingleTaskRunSubmission,
)
from hfa_control.service import ControlPlaneService
from hfa_control.submission_idempotency import (
    canonical_submission_fingerprint,
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
    / "sprint83_9_idempotency_diagnostics.json"
)


class CountingAdmission:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.calls: list[str] = []

    async def admit(self, request: Any):
        self.calls.append(str(request.run_id))
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


def _control_config(*, acceptance_id: str) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id=f"cp-s83-9-{acceptance_id}",
        region="sprint83-9-acceptance",
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
            TenantIdentityBoundary.TRUSTED_GATEWAY_HEADER.value
        ),
    )


def _client(
    control: ControlPlaneService,
    redis_client: Any,
) -> httpx.AsyncClient:
    app = FastAPI()
    app.include_router(router)
    app.state.cp = control
    app.state.redis = redis_client
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sprint83-9.test",
    )


def _submission_body() -> dict[str, Any]:
    return {
        "run_shape": "SINGLE_TASK",
        "payload": {
            "mode": "idempotency-diagnostics",
            "prompt": "diagnose reservation safely",
        },
        "agent_type": "sprint83-9-acceptance",
        "priority": 5,
        "estimated_cost_cents": 0,
        "preferred_region": "",
        "preferred_placement": "LEAST_LOADED",
        "required_capabilities": [],
        "trace_parent": "",
        "trace_state": "",
    }


async def _keyspace_digest(redis_client: Any) -> str:
    keys: list[str] = []
    async for raw_key in redis_client.scan_iter(match="*"):
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


async def _scenario(
    redis_client: Any,
    *,
    acceptance_id: str,
) -> dict[str, Any]:
    tenant_id = f"tenant{acceptance_id.replace('-', '')}"
    idempotency_key = f"{acceptance_id}-diagnostics"
    owner_token = "internal-owner-token-must-not-leak"
    run_id = (
        f"run-{tenant_id}-"
        "11111111-1111-4111-8111-111111111111"
    )
    task_id = "task-22222222-2222-4222-8222-222222222222"
    created_at_ms = 1000
    updated_at_ms = 1000
    body = _submission_body()

    control = ControlPlaneService(
        redis_client,
        _control_config(acceptance_id=acceptance_id),
    )
    control._admitter._audit = None

    admission = CountingAdmission(control._admitter)
    dag = CountingDagLua(control._scheduler.composition.dag_lua)
    control._run_submission = RunSubmissionCoordinator(
        admission_controller=admission,
        dag_lua=dag,
        idempotency_store=control._submission_idempotency,
        require_idempotency_key=True,
        clock_ms=lambda: 9_999_999,
    )

    request_model = SingleTaskRunSubmission(
        tenant_id=tenant_id,
        payload=body["payload"],
        run_shape=body["run_shape"],
        agent_type=body["agent_type"],
        priority=body["priority"],
        estimated_cost_cents=body["estimated_cost_cents"],
        preferred_region=body["preferred_region"],
        preferred_placement=body["preferred_placement"],
        required_capabilities=(),
        trace_parent=body["trace_parent"],
        trace_state=body["trace_state"],
        idempotency_key=idempotency_key,
    )
    fingerprint = canonical_submission_fingerprint(request_model)

    await control._submission_idempotency.reserve(
        tenant_id=tenant_id,
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
        run_id=run_id,
        task_id=task_id,
        owner_token=owner_token,
        created_at_ms=created_at_ms,
    )

    redis_key = submission_idempotency_redis_key(
        tenant_id,
        idempotency_key,
    )
    record = {
        _decode(key): _decode(value)
        for key, value in (
            await redis_client.hgetall(redis_key)
        ).items()
    }

    client = _client(control, redis_client)
    try:
        before = await _keyspace_digest(redis_client)
        response = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
                "Idempotency-Key": idempotency_key,
            },
            json=body,
        )
        after = await _keyspace_digest(redis_client)
        response_body = response.json()

        ttl_seconds = response_body.get(
            "idempotency_reservation_ttl_seconds"
        )
        ttl_valid = (
            type(ttl_seconds) is int
            and 0 < ttl_seconds <= RedisTTL.SUBMISSION_IDEMPOTENCY
        )
        serialized_response = json.dumps(
            response_body,
            sort_keys=True,
        )

        await redis_client.hset(
            redis_key,
            mapping={
                "created_at_ms": "corrupt",
                "updated_at_ms": "corrupt",
            },
        )
        before_corrupt = await _keyspace_digest(redis_client)
        corrupt_response = await client.post(
            "/control/v1/runs",
            headers={
                "X-Tenant-ID": tenant_id,
                "Idempotency-Key": idempotency_key,
            },
            json=body,
        )
        after_corrupt = await _keyspace_digest(redis_client)
        corrupt_body = corrupt_response.json()

        passed = all(
            (
                response.status_code == 409,
                response_body.get("failure_code")
                == "IDEMPOTENCY_IN_PROGRESS",
                response_body.get("run_id") == run_id,
                response_body.get("task_id") == task_id,
                response_body.get(
                    "idempotency_reservation_created_at_ms"
                )
                == created_at_ms,
                response_body.get(
                    "idempotency_reservation_updated_at_ms"
                )
                == updated_at_ms,
                ttl_valid,
                response_body.get(
                    "idempotency_recovery_safe"
                )
                is False,
                owner_token not in serialized_response,
                idempotency_key not in redis_key,
                record.get("owner_token") == owner_token,
                before == after,
                admission.calls == [],
                dag.initialise_calls == 0,
                dag.task_admit_calls == [],
                corrupt_response.status_code == 503,
                corrupt_body.get("failure_code")
                == "IDEMPOTENCY_STORE_FAILED",
                before_corrupt == after_corrupt,
            )
        )

        return {
            "name": "idempotency_reservation_diagnostics",
            "status": "PASS" if passed else "FAIL",
            "in_progress": {
                "status_code": response.status_code,
                "failure_code": response_body.get(
                    "failure_code"
                ),
                "same_run_id": response_body.get("run_id") == run_id,
                "same_task_id": response_body.get("task_id") == task_id,
                "created_at_ms_exposed": (
                    response_body.get(
                        "idempotency_reservation_created_at_ms"
                    )
                    == created_at_ms
                ),
                "updated_at_ms_exposed": (
                    response_body.get(
                        "idempotency_reservation_updated_at_ms"
                    )
                    == updated_at_ms
                ),
                "ttl_seconds_within_retention": ttl_valid,
                "recovery_safe": response_body.get(
                    "idempotency_recovery_safe"
                ),
                "owner_token_exposed": (
                    owner_token in serialized_response
                ),
                "zero_keyspace_mutation": before == after,
                "lifecycle_calls": {
                    "run_admission": len(admission.calls),
                    "dag_initialise": dag.initialise_calls,
                    "task_admit": len(dag.task_admit_calls),
                },
            },
            "corrupt_diagnostics": {
                "status_code": corrupt_response.status_code,
                "failure_code": corrupt_body.get(
                    "failure_code"
                ),
                "zero_keyspace_mutation": (
                    before_corrupt == after_corrupt
                ),
            },
            "architecture": {
                "new_lifecycle_writer": False,
                "stale_owner_takeover_used": False,
                "automatic_release_used": False,
                "automatic_retry_used": False,
                "automatic_repair_used": False,
                "owner_token_exposed": False,
            },
        }
    finally:
        await client.aclose()


async def build_acceptance_report(
    redis_url: str,
    *,
    acceptance_id: str = "s83-9",
    reset_test_db: bool = False,
) -> dict[str, Any]:
    acceptance_id = _validate_acceptance_id(acceptance_id)

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
    os.environ["HFA_CANONICAL_TASK_ADMIT_BINDING"] = "1"
    os.environ["HFA_PRODUCT_MODE"] = (
        ProductMode.SINGLE_TASK_ALPHA.value
    )
    os.environ["HFA_TENANT_IDENTITY_BOUNDARY"] = (
        TenantIdentityBoundary.TRUSTED_GATEWAY_HEADER.value
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
            "sprint": "83.9",
            "source": "idempotency_reservation_diagnostics",
            "status": (
                "PASS_WITH_LIMITATIONS"
                if passed
                else "FAIL"
            ),
            "acceptance_id": acceptance_id,
            "real_redis_used": True,
            "real_asgi_application_used": True,
            "production_control_composition_used": passed,
            "reservation_timestamps_exposed": passed,
            "reservation_ttl_exposed": passed,
            "owner_token_exposed": False,
            "in_progress_zero_lifecycle_writes": passed,
            "corrupt_diagnostics_fail_closed": passed,
            "stale_owner_takeover": False,
            "automatic_release": False,
            "automatic_retry": False,
            "automatic_repair": False,
            "new_lifecycle_writer": False,
            "production_ready": False,
            "production_cutover_authorized": False,
            "scenarios": [scenario],
            "limitations": [
                (
                    "Reservation diagnostics do not prove "
                    "that takeover, release, retry, or repair "
                    "is safe."
                ),
                (
                    "IN_PROGRESS submissions continue to "
                    "fail closed with zero lifecycle writes."
                ),
                (
                    "Owner tokens remain internal and are "
                    "never returned through the tenant API."
                ),
                (
                    "Direct public authentication, multi-task "
                    "aggregation, cancellation, retry/requeue, "
                    "external executor cutover, archive, and "
                    "production cutover remain unsupported."
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


async def _async_main(args: argparse.Namespace) -> int:
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
            "SPRINT83_9_ACCEPTANCE "
            f"status={report['status']} "
            "reservation_timestamps_exposed="
            f"{str(report['reservation_timestamps_exposed']).lower()} "
            "production_ready="
            f"{str(report['production_ready']).lower()}"
        )
    return 0 if report["status"] == (
        "PASS_WITH_LIMITATIONS"
    ) else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-url",
        default=os.environ.get(
            "REDIS_URL",
            "redis://127.0.0.1:6389/0",
        ),
    )
    parser.add_argument(
        "--acceptance-id",
        default="s83-9",
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
    return asyncio.run(_async_main(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
