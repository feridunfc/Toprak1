from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.runtime_alpha_acceptance_83_7 import (
    PUBLIC_FAILURE,
    build_acceptance_report,
)


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


async def test_trusted_gateway_product_alpha_report_83_7():
    report = await build_acceptance_report(
        os.environ.get(
            "REDIS_URL",
            "redis://127.0.0.1:6389/0",
        ),
        acceptance_id="s83-7-integration",
        reset_test_db=True,
    )

    assert report["schema_version"] == 1
    assert report["sprint"] == "83.7"
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert report["real_redis_used"] is True
    assert report["real_asgi_application_used"] is True
    assert report["network_socket_http_used"] is False
    assert report["production_scheduler_used"] is True
    assert report["production_worker_used"] is True
    assert report["canonical_task_admit_used"] is True
    assert report["run_finalization_supported"] is True
    assert report["product_readiness_supported"] is True
    assert report["capability_contract_supported"] is True
    assert report["success_path"] is True
    assert report["failure_path"] is True
    assert report[
        "canonical_task_running_observed"
    ] is True
    assert report["public_run_running_supported"] is False
    assert report["sanitized_failure_contract"] is True
    assert report[
        "control_service_restart_durability"
    ] is True
    assert report[
        "worker_service_restart_durability"
    ] is True
    assert report["duplicate_get_zero_writes"] is True
    assert report["duplicate_post_distinct_run"] is True
    assert report["tenant_isolation"] is True
    assert report[
        "unsupported_multi_task_zero_writes"
    ] is True
    assert report["internal_product_alpha_ready"] is True
    assert report[
        "trusted_gateway_product_alpha_ready"
    ] is True
    assert report["product_alpha_ready"] is True

    assert report[
        "direct_public_multitenant_alpha_ready"
    ] is False
    assert report["production_ready"] is False
    assert report[
        "submission_idempotency_supported"
    ] is False
    assert report["multi_task_support"] is False
    assert report["cancel_supported"] is False
    assert report["retry_supported"] is False
    assert report["external_executor_cutover"] is False
    assert report["automatic_repair"] is False
    assert report["archive_available"] is False
    assert report["new_lifecycle_writer"] is False
    assert report["direct_lifecycle_write_used"] is False
    assert report["direct_dispatch_call_used"] is False
    assert report["Loop_Plane_dependencies"] == 0
    assert report["production_cutover_authorized"] is False

    scenario = report["scenarios"][0]
    assert scenario["status"] == "PASS"
    assert (
        scenario["initial_product_readiness"]["ready"]
        is True
    )
    assert (
        scenario["initial_product_readiness"][
            "product_mode"
        ]
        == "SINGLE_TASK_ALPHA"
    )
    assert (
        scenario["initial_product_readiness"][
            "tenant_identity_boundary"
        ]
        == "TRUSTED_GATEWAY_HEADER"
    )
    assert scenario["success"]["queued_status"] == "QUEUED"
    assert (
        scenario["success"]["task_running_state"]
        == "running"
    )
    assert (
        scenario["success"][
            "public_run_status_while_task_running"
        ]
        == "QUEUED"
    )
    assert (
        scenario["success"]["terminal_status"]
        == "COMPLETED"
    )
    assert scenario["success"]["terminal_outcome"] == "SUCCESS"
    assert scenario["success"]["task_output"] == {
        "output_text": "SPRINT83_7_ALPHA_SUCCESS"
    }
    assert (
        scenario["failure"]["task_running_state"]
        == "running"
    )
    assert (
        scenario["failure"][
            "public_run_status_while_task_running"
        ]
        == "QUEUED"
    )
    assert (
        scenario["failure"]["terminal_status"]
        == "FAILED"
    )
    assert scenario["failure"]["terminal_outcome"] == "FAILURE"
    assert scenario["failure"]["public_error"] == PUBLIC_FAILURE
    assert (
        scenario["failure"]["public_task_output"]
        == PUBLIC_FAILURE
    )
    assert scenario["failure"][
        "raw_failure_output_contains_private"
    ] is False
    assert scenario["failure"][
        "public_response_contains_private"
    ] is False
    assert scenario["failure"]["legacy_error"] == (
        "Task execution failed."
    )
    assert scenario["contract"][
        "unsupported_multi_task_status_code"
    ] == 400
    assert scenario["contract"][
        "unsupported_multi_task_failure_code"
    ] == "UNSUPPORTED_RUN_SHAPE"
    assert scenario["contract"][
        "unsupported_multi_task_zero_new_keys"
    ] is True
    assert scenario["contract"][
        "duplicate_post_distinct_run"
    ] is True
    assert scenario["contract"][
        "duplicate_get_zero_writes"
    ] is True
    assert scenario["contract"][
        "tenant_isolation_status_code"
    ] == 403
    assert scenario["contract"][
        "unknown_run_status_code"
    ] == 404
    assert scenario["durability"][
        "control_service_restart_semantics_stable"
    ] is True
    assert scenario["durability"][
        "worker_service_restart_semantics_stable"
    ] is True
    assert scenario["durability"][
        "replacement_executor_call_count"
    ] == 0
    assert scenario["durability"][
        "original_executor_call_count"
    ] == 4
    assert scenario["durability"]["pending_count"] == 0


async def test_trusted_gateway_product_alpha_cli_83_7(
    tmp_path,
):
    root = Path(__file__).resolve().parents[2]
    output = (
        tmp_path
        / "sprint83_7_trusted_gateway_product_alpha.json"
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(
                root
                / "scripts"
                / "runtime_alpha_acceptance_83_7.py"
            ),
            "--redis-url",
            os.environ.get(
                "REDIS_URL",
                "redis://127.0.0.1:6389/0",
            ),
            "--acceptance-id",
            "s83-7-cli",
            "--reset-test-db",
            "--out",
            str(output),
            "--json",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )

    assert completed.returncode == 0, (
        completed.stdout + completed.stderr
    )
    report = json.loads(
        output.read_text(encoding="utf-8")
    )
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert report[
        "trusted_gateway_product_alpha_ready"
    ] is True
    assert report["product_alpha_ready"] is True
    assert report["production_ready"] is False
    assert report["scenarios"][0]["status"] == "PASS"
