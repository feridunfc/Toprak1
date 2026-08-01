from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.runtime_alpha_acceptance_83_6 import (
    build_acceptance_report,
)


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


async def test_user_facing_task_output_acceptance_report_83_6():
    report = await build_acceptance_report(
        os.environ.get(
            "REDIS_URL",
            "redis://127.0.0.1:6389/0",
        ),
        acceptance_id="s83-6-integration",
        reset_test_db=True,
    )

    assert report["schema_version"] == 1
    assert report["sprint"] == "83.6"
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
    ] is True
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
    assert (
        scenario["http_terminal_status"]
        == "COMPLETED"
    )
    assert (
        scenario["http_terminal_outcome"]
        == "SUCCESS"
    )
    assert (
        scenario["http_completeness"]
        == "TERMINAL_WITH_RESULT"
    )
    assert (
        scenario["http_task_output_status"]
        == "AVAILABLE"
    )
    assert scenario["http_task_id"] == scenario["task_id"]
    assert scenario["http_task_state"] == "done"
    assert scenario[
        "http_task_output_matches_durable"
    ] is True
    assert (
        scenario["http_task_output_issue_count"]
        == 0
    )
    assert scenario["http_task_output"] == {
        "output_text": "SPRINT83_6_HTTP_OUTPUT_OK"
    }
    assert (
        scenario["task_output_text"]
        == "SPRINT83_6_HTTP_OUTPUT_OK"
    )
    assert scenario["aggregate_task_count"] == 1
    assert scenario["aggregate_done_count"] == 1
    assert scenario["terminal_event_count"] == 1
    assert (
        scenario["terminal_event_type"]
        == "RunCompleted"
    )
    assert scenario["pending_count"] == 0
    assert (
        scenario["running_projection_cleared"]
        is True
    )
    assert scenario["executor_call_count"] == 1


async def test_user_facing_task_output_acceptance_cli_83_6(
    tmp_path,
):
    root = Path(__file__).resolve().parents[2]
    output = (
        tmp_path
        / "sprint83_6_single_task_output_read.json"
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(
                root
                / "scripts"
                / "runtime_alpha_acceptance_83_6.py"
            ),
            "--redis-url",
            os.environ.get(
                "REDIS_URL",
                "redis://127.0.0.1:6389/0",
            ),
            "--acceptance-id",
            "s83-6-cli",
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
        "task_output_exposed_in_http_result"
    ] is True
    scenario = report["scenarios"][0]
    assert (
        scenario["http_task_output_status"]
        == "AVAILABLE"
    )
    assert scenario[
        "http_task_output_matches_durable"
    ] is True
