from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.runtime_alpha_acceptance import build_acceptance_report


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6389/15")

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_runtime_alpha_core_executes_and_reports_known_limits() -> None:
    report = await build_acceptance_report(
        REDIS_URL,
        acceptance_id="s83-1-integration",
        reset_test_db=True,
    )

    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert report["runtime_alpha_testable"] is True
    assert report["product_alpha_ready"] is False
    assert report["production_ready"] is False
    assert report["real_redis_used"] is True
    assert report["canonical_task_admit_used"] is True
    assert report["runtime_truth_guarded_dispatch_used"] is True
    assert report["worker_consumer_used"] is True
    assert report["task_consumer_used"] is True
    assert report["task_output_readable"] is True
    assert report["conflict_explanation_available"] is True
    assert report["run_finalization_supported"] is False
    assert report["cancel_command_supported"] is False
    assert report["retry_command_supported"] is False
    assert report["automatic_repair_authorized"] is False
    assert report["production_cutover_authorized"] is False

    scenarios = {row["name"]: row for row in report["scenarios"]}
    assert set(scenarios) == {
        "canonical_task_success",
        "terminal_run_blocks_dispatch",
    }

    healthy = scenarios["canonical_task_success"]
    assert healthy["status"] == "PASS"
    assert healthy["task_admitted"] is True
    assert healthy["task_ready"] is True
    assert healthy["reservation_created"] is True
    assert healthy["dispatch_committed"] is True
    assert healthy["executor_calls"] == 1
    assert healthy["task_completed"] is True
    assert healthy["task_output_readable"] is True
    assert healthy["output"]["output_text"] == "ALPHA_ECHO: runtime alpha acceptance"
    assert healthy["message_acknowledged"] is True
    assert healthy["pending_count"] == 0
    assert healthy["claim_epoch_present"] is True
    assert healthy["run_state"] == "running"
    assert healthy["run_truth_status"] == "conflict"
    assert healthy["run_finalization_gap"] is True

    conflict = scenarios["terminal_run_blocks_dispatch"]
    assert conflict["status"] == "PASS"
    assert conflict["dispatch_committed"] is False
    assert conflict["dispatch_status"] == "run_truth_terminal_conflict"
    assert conflict["dispatch_reason"] == "run_state_terminal"
    assert conflict["no_lifecycle_mutation"] is True
    assert conflict["conflict_explained"] is True
    assert len(conflict["conflicts"]) == 1
    assert conflict["conflicts"][0]["operation"] == "TASK_DISPATCH"
    assert conflict["conflicts"][0]["detail_code"] == "run_state_terminal"


async def test_runtime_alpha_report_is_deterministic_for_fixed_identity() -> None:
    first = await build_acceptance_report(
        REDIS_URL,
        acceptance_id="s83-1-deterministic",
        reset_test_db=True,
    )
    second = await build_acceptance_report(
        REDIS_URL,
        acceptance_id="s83-1-deterministic",
        reset_test_db=True,
    )

    assert first == second
    assert first["acceptance_id"] == "s83-1-deterministic"
    assert first["global_truth_policy"] == "OPERATION_SCOPED_FAIL_CLOSED"
    assert len(first["scenarios"]) == 2
    assert len(first["limitations"]) == 5


async def test_runtime_alpha_one_command_cli_writes_report(tmp_path: Path) -> None:
    output = tmp_path / "runtime-alpha.json"
    repo_root = Path(__file__).resolve().parents[2]
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
            "scripts/runtime_alpha_acceptance.py",
            "--redis-url",
            REDIS_URL,
            "--acceptance-id",
            "s83-1-cli",
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
    written = json.loads(output.read_text(encoding="utf-8"))
    assert stdout == written
    assert written["status"] == "PASS_WITH_LIMITATIONS"
    assert written["runtime_alpha_testable"] is True
    assert written["product_alpha_ready"] is False
    assert written["one_command_acceptance_available"] is True
