from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.runtime_alpha_acceptance_83_2 import build_acceptance_report


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6389/15")
pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_runtime_alpha_finalizes_run_and_keeps_known_limits() -> None:
    report = await build_acceptance_report(
        REDIS_URL,
        acceptance_id="s83-2-integration",
        reset_test_db=True,
    )

    assert report["schema_version"] == 2
    assert report["sprint"] == "83.2"
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert report["runtime_alpha_testable"] is True
    assert report["product_alpha_ready"] is False
    assert report["production_ready"] is False
    assert report["run_finalization_supported"] is True
    assert report["run_result_visible"] is True
    assert report["run_terminal_event_visible"] is True
    assert report["run_termination_binding_default_enabled"] is False
    assert report["run_termination_binding_test_enabled"] is True
    assert report["cancel_command_supported"] is False
    assert report["retry_command_supported"] is False
    assert report["automatic_repair_authorized"] is False
    assert report["production_cutover_authorized"] is False

    scenarios = {row["name"]: row for row in report["scenarios"]}
    assert set(scenarios) == {
        "canonical_task_success_finalizes_run",
        "terminal_run_blocks_dispatch",
    }

    healthy = scenarios["canonical_task_success_finalizes_run"]
    assert healthy["status"] == "PASS"
    assert healthy["task_completed"] is True
    assert healthy["task_output_readable"] is True
    assert healthy["message_acknowledged"] is True
    assert healthy["pending_count"] == 0
    assert healthy["run_state"] == "done"
    assert healthy["run_truth_status"] == "consistent"
    assert healthy["run_truth_conflicts"] == []
    assert healthy["run_finalized"] is True
    assert healthy["run_result_visible"] is True
    assert healthy["run_result"]["status"] == "done"
    assert healthy["run_result"]["finalization_operation"] == "RUN_TERMINATE"
    assert healthy["run_terminal_event_visible"] is True
    assert healthy["running_projection_cleared"] is True
    assert healthy["run_meta_converged"] is True

    conflict = scenarios["terminal_run_blocks_dispatch"]
    assert conflict["status"] == "PASS"
    assert conflict["dispatch_committed"] is False
    assert conflict["dispatch_status"] == "run_truth_terminal_conflict"
    assert conflict["no_lifecycle_mutation"] is True
    assert conflict["conflict_explained"] is True


async def test_runtime_alpha_83_2_report_is_deterministic_for_fixed_identity() -> None:
    first = await build_acceptance_report(
        REDIS_URL,
        acceptance_id="s83-2-deterministic",
        reset_test_db=True,
    )
    second = await build_acceptance_report(
        REDIS_URL,
        acceptance_id="s83-2-deterministic",
        reset_test_db=True,
    )

    assert first == second
    assert first["acceptance_id"] == "s83-2-deterministic"
    assert first["global_truth_policy"] == "OPERATION_SCOPED_FAIL_CLOSED"
    assert len(first["scenarios"]) == 2
    assert len(first["limitations"]) == 4


async def test_runtime_alpha_83_2_one_command_cli_writes_report(tmp_path: Path) -> None:
    output = tmp_path / "runtime-alpha-83-2.json"
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
            "scripts/runtime_alpha_acceptance_83_2.py",
            "--redis-url",
            REDIS_URL,
            "--acceptance-id",
            "s83-2-cli",
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
    assert written["run_finalization_supported"] is True
    assert written["product_alpha_ready"] is False
    assert written["production_cutover_authorized"] is False
