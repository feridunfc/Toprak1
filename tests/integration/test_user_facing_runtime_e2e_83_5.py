from __future__ import annotations

from uuid import UUID

import pytest

from hfa.authority import (
    AggregateType,
    CanonicalAggregateIdentity,
)
from hfa.dag.schema import DagRedisKey
from hfa.config.keys import RedisKey
from hfa_control.models import ControlPlaneConfig
from hfa_control.run_submission import (
    RunSubmissionCoordinator,
    SingleTaskRunSubmission,
)
from hfa_control.service import ControlPlaneService
from hfa_tools.middleware.tenant import validate_run_id_format


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


class UUIDFactory:
    def __init__(self, *values: UUID) -> None:
        self._values = iter(values)

    def __call__(self) -> UUID:
        return next(self._values)


def request() -> SingleTaskRunSubmission:
    return SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload={
            "prompt": "real redis submission",
            "nested": {"b": 2, "a": 1},
        },
        agent_type="fake",
        priority=5,
        estimated_cost_cents=0,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
    )


def config() -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id="cp-sprint83-5-integration",
        stream_shards=1,
        strict_cas_mode=True,
    )


async def test_real_redis_submission_creates_canonical_ready_root(
    redis_client,
    monkeypatch,
):
    monkeypatch.setenv(
        "HFA_CANONICAL_TASK_ADMIT_BINDING",
        "1",
    )
    service = ControlPlaneService(
        redis_client,
        config(),
    )

    result = await service.submit_single_task_run(
        request()
    )

    assert result["status"] == "ACCEPTED"
    assert result["run_admitted"] is True
    assert result["task_admitted"] is True
    assert result["task_ready"] is True
    assert result["task_admit_status"] == "seeded_root"
    assert result["dispatch_possible"] is True

    run_id = result["run_id"]
    task_id = result["task_id"]
    parsed_tenant, parsed_uuid = validate_run_id_format(
        run_id
    )
    assert parsed_tenant == "tenant1"
    assert parsed_uuid

    assert (
        await redis_client.get(RedisKey.run_state(run_id))
        == "admitted"
    )
    assert (
        await redis_client.get(
            DagRedisKey.task_state(task_id)
        )
        == "ready"
    )

    task_meta = await redis_client.hgetall(
        DagRedisKey.task_meta(task_id)
    )
    assert task_meta["task_id"] == task_id
    assert task_meta["run_id"] == run_id
    assert task_meta["tenant_id"] == "tenant1"
    assert task_meta["agent_type"] == "fake"
    assert task_meta["priority"] == "5"
    assert (
        task_meta["policy"]
        == "LEAST_LOADED"
    )
    assert (
        task_meta["payload_json"]
        == '{"nested":{"a":1,"b":2},"prompt":"real redis submission"}'
    )

    assert await redis_client.sismember(
        DagRedisKey.run_tasks(run_id),
        task_id,
    )
    assert await redis_client.sismember(
        DagRedisKey.tenant_active_set(),
        "tenant1",
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_ready_queue("tenant1"),
            task_id,
        )
        is not None
    )

    dag_lua = service._scheduler.composition.dag_lua
    assert (
        dag_lua
        is service._run_submission._dag_lua
    )
    assert (
        dag_lua._canonical_task_admit_binding_enabled
        is True
    )
    binding = dag_lua._task_admit_authority_binding
    assert binding is not None
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=run_id,
        task_id=task_id,
    )
    snapshot = await binding.store.get_aggregate_snapshot(
        identity
    )
    assert snapshot is not None
    assert snapshot.revision == 1
    assert snapshot.state == "ready"

    read = await service.get_run_status_result(run_id)
    assert read["run_id"] == run_id
    assert read["status"] == "QUEUED"
    assert read["terminal"] is False
    assert (
        read["completeness"]
        == "RUNNING_WITHOUT_RESULT"
    )

    await service._scheduler.close()


async def test_real_redis_task_collision_leaves_explicit_incomplete_run(
    redis_client,
    monkeypatch,
):
    monkeypatch.setenv(
        "HFA_CANONICAL_TASK_ADMIT_BINDING",
        "1",
    )
    service = ControlPlaneService(
        redis_client,
        config(),
    )

    run_uuid = UUID(
        "11111111-1111-4111-8111-111111111111"
    )
    task_uuid = UUID(
        "22222222-2222-4222-8222-222222222222"
    )
    task_id = f"task-{task_uuid}"
    await redis_client.set(
        DagRedisKey.task_state(task_id),
        "ready",
    )

    service._run_submission = RunSubmissionCoordinator(
        admission_controller=service._admitter,
        dag_lua=service._scheduler.composition.dag_lua,
        uuid_factory=UUIDFactory(
            run_uuid,
            task_uuid,
        ),
        clock_ms=lambda: 1_700_000_000_123,
    )

    result = await service.submit_single_task_run(
        request()
    )

    assert result["status"] == "SUBMISSION_INCOMPLETE"
    assert result["failure_code"] == (
        "TASK_ADMISSION_FAILED"
    )
    assert result["failure_type"] == (
        "TaskAdmitLegacyStateConflictError"
    )
    assert result["run_admitted"] is True
    assert result["task_admitted"] is False
    assert result["task_ready"] is False
    assert result["dispatch_possible"] is False
    assert result["automatic_retry"] is False
    assert result["automatic_rollback"] is False
    assert result["automatic_repair"] is False

    run_id = result["run_id"]
    assert (
        await redis_client.get(RedisKey.run_state(run_id))
        == "admitted"
    )
    assert (
        await redis_client.get(
            DagRedisKey.task_state(task_id)
        )
        == "ready"
    )
    assert not await redis_client.sismember(
        DagRedisKey.run_tasks(run_id),
        task_id,
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_ready_queue("tenant1"),
            task_id,
        )
        is None
    )

    await service._scheduler.close()


async def test_user_facing_runtime_acceptance_report_83_5():
    import os

    from scripts.runtime_alpha_acceptance_83_5 import (
        build_acceptance_report,
    )

    report = await build_acceptance_report(
        os.environ.get(
            "REDIS_URL",
            "redis://127.0.0.1:6389/0",
        ),
        acceptance_id="s83-5-integration",
        reset_test_db=True,
    )

    assert report["schema_version"] == 1
    assert report["sprint"] == "83.5"
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert report[
        "user_facing_http_submit_supported"
    ] is True
    assert report[
        "user_facing_http_status_result_supported"
    ] is True
    assert report["production_scheduler_used"] is True
    assert report["production_worker_used"] is True
    assert report["canonical_task_admit_used"] is True
    assert report["run_finalization_supported"] is True
    assert report["terminal_http_read_supported"] is True
    assert report["task_output_durable"] is True
    assert report[
        "task_output_exposed_in_http_result"
    ] is False
    assert report["new_lifecycle_writer"] is False
    assert report["direct_lifecycle_write_used"] is False
    assert report["direct_dispatch_call_used"] is False
    assert report["Loop_Plane_dependencies"] == 0
    assert report["product_alpha_ready"] is False
    assert report["production_ready"] is False
    assert report["production_cutover_authorized"] is False

    scenario = report["scenarios"][0]
    assert scenario["status"] == "PASS"
    assert scenario["http_submit_status_code"] == 202
    assert scenario["http_submit_status"] == "ACCEPTED"
    assert scenario["scheduler_running"] is True
    assert scenario["worker_ready"] is True
    assert scenario["task_state"] == "done"
    assert scenario["run_state"] == "done"
    assert scenario["http_terminal_status"] == "COMPLETED"
    assert scenario["http_terminal_outcome"] == "SUCCESS"
    assert (
        scenario["http_completeness"]
        == "TERMINAL_WITH_RESULT"
    )
    assert scenario["aggregate_task_count"] == 1
    assert scenario["aggregate_done_count"] == 1
    assert (
        scenario["task_output_text"]
        == "SPRINT83_5_USER_FACING_E2E_OK"
    )
    assert scenario["terminal_event_count"] == 1
    assert scenario["terminal_event_type"] == "RunCompleted"
    assert scenario["pending_count"] == 0
    assert scenario["running_projection_cleared"] is True
    assert scenario["executor_call_count"] == 1


async def test_user_facing_runtime_acceptance_cli_83_5(
    tmp_path,
):
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    output = tmp_path / "sprint83_5_acceptance.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(
                root
                / "scripts"
                / "runtime_alpha_acceptance_83_5.py"
            ),
            "--redis-url",
            os.environ.get(
                "REDIS_URL",
                "redis://127.0.0.1:6389/0",
            ),
            "--acceptance-id",
            "s83-5-cli",
            "--reset-test-db",
            "--out",
            str(output),
            "--json",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        timeout=45,
    )

    assert completed.returncode == 0, (
        completed.stdout + completed.stderr
    )
    report = json.loads(
        output.read_text(encoding="utf-8")
    )
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert report[
        "user_facing_http_submit_supported"
    ] is True
    assert report["production_scheduler_used"] is True
    assert report["production_worker_used"] is True
    assert report["product_alpha_ready"] is False
