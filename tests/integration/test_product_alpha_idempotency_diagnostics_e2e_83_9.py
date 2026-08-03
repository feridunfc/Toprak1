from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.runtime_alpha_acceptance_83_9 import (
    build_acceptance_report,
)


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


async def test_idempotency_diagnostics_report_83_9():
    report = await build_acceptance_report(
        os.environ.get(
            "REDIS_URL",
            "redis://127.0.0.1:6389/0",
        ),
        acceptance_id="s83-9-integration",
        reset_test_db=True,
    )

    assert report["schema_version"] == 1
    assert report["sprint"] == "83.9"
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert report["real_redis_used"] is True
    assert report["real_asgi_application_used"] is True
    assert report["production_control_composition_used"] is True
    assert report["reservation_timestamps_exposed"] is True
    assert report["reservation_ttl_exposed"] is True
    assert report["owner_token_exposed"] is False
    assert report["in_progress_zero_lifecycle_writes"] is True
    assert report["corrupt_diagnostics_fail_closed"] is True
    assert report["stale_owner_takeover"] is False
    assert report["automatic_release"] is False
    assert report["automatic_retry"] is False
    assert report["automatic_repair"] is False
    assert report["new_lifecycle_writer"] is False
    assert report["production_ready"] is False
    assert report["production_cutover_authorized"] is False

    scenario = report["scenarios"][0]
    assert scenario["status"] == "PASS"

    in_progress = scenario["in_progress"]
    assert in_progress["status_code"] == 409
    assert in_progress["failure_code"] == "IDEMPOTENCY_IN_PROGRESS"
    assert in_progress["same_run_id"] is True
    assert in_progress["same_task_id"] is True
    assert in_progress["created_at_ms_exposed"] is True
    assert in_progress["updated_at_ms_exposed"] is True
    assert in_progress["ttl_seconds_within_retention"] is True
    assert in_progress["recovery_safe"] is False
    assert in_progress["owner_token_exposed"] is False
    assert in_progress["zero_keyspace_mutation"] is True
    assert in_progress["lifecycle_calls"] == {
        "run_admission": 0,
        "dag_initialise": 0,
        "task_admit": 0,
    }

    corrupt = scenario["corrupt_diagnostics"]
    assert corrupt["status_code"] == 503
    assert corrupt["failure_code"] == "IDEMPOTENCY_STORE_FAILED"
    assert corrupt["zero_keyspace_mutation"] is True


async def test_idempotency_diagnostics_cli_83_9(tmp_path):
    root = Path(__file__).resolve().parents[2]
    output = (
        tmp_path
        / "sprint83_9_idempotency_diagnostics.json"
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(
                root
                / "scripts"
                / "runtime_alpha_acceptance_83_9.py"
            ),
            "--redis-url",
            os.environ.get(
                "REDIS_URL",
                "redis://127.0.0.1:6389/0",
            ),
            "--acceptance-id",
            "s83-9-cli",
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
    assert report["reservation_timestamps_exposed"] is True
    assert report["reservation_ttl_exposed"] is True
    assert report["owner_token_exposed"] is False
    assert report["stale_owner_takeover"] is False
    assert report["production_ready"] is False
