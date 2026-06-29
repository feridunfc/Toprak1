import json
import os
import subprocess
import sys
from pathlib import Path

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6389/0")

ARTIFACT_PATH = Path("docs/dashboard/artifacts/latest_thin_product_task_cli_demo.json")


def run_command(args):
    env = os.environ.copy()
    repo_root = str(Path(__file__).resolve().parents[2])
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        repo_root if not existing_pythonpath else repo_root + os.pathsep + existing_pythonpath
    )

    completed = subprocess.run(
        [sys.executable, *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=repo_root,
    )

    if completed.returncode != 0:
        raise AssertionError(
            "Command failed\n"
            f"returncode={completed.returncode}\n"
            f"cmd={[sys.executable, *args]}\n"
            f"stdout={completed.stdout}\n"
            f"stderr={completed.stderr}\n"
        )

    return json.loads(completed.stdout)


def test_ironclad_submit_cli_returns_user_visible_submission():
    payload = run_command(
        [
            "scripts/ironclad_submit.py",
            "--tenant",
            "demo",
            "--message",
            "Hello IRONCLAD",
            "--redis-url",
            REDIS_URL,
            "--json",
        ]
    )

    assert payload["source"] == "ironclad_submit"
    assert payload["status"] == "SUBMITTED"
    assert payload["product_visible"] is True
    assert payload["tenant_id"] == "demo"
    assert payload["task_id"]
    assert payload["run_id"]
    assert payload["message"] == "Hello IRONCLAD"
    assert payload["runtime_claim"] == "USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND"

    assert payload["production_llm_call_attempted"] is False
    assert payload["deployment_attempted"] is False
    assert payload["release_tag_created"] is False
    assert payload["operator_action_buttons"] is False


def test_ironclad_demo_cli_executes_sprint_42_runtime_and_writes_artifact():
    if ARTIFACT_PATH.exists():
        ARTIFACT_PATH.unlink()

    payload = run_command(
        [
            "scripts/ironclad_demo.py",
            "--tenant",
            "demo",
            "--message",
            "Hello IRONCLAD",
            "--redis-url",
            REDIS_URL,
            "--json",
        ]
    )

    assert payload["source"] == "thin_product_task_cli_demo"
    assert payload["status"] == "PASS"
    assert payload["product_visible"] is True
    assert payload["runtime_claim"] == "USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND"
    assert payload["uses_sprint_42_runtime"] is True

    assert payload["submit_cli_available"] is True
    assert payload["result_cli_available"] is True
    assert payload["demo_cli_available"] is True

    assert payload["tenant_id"] == "tenant-demo"
    assert payload["task_id"]
    assert payload["run_id"]
    assert payload["message"] == "Hello IRONCLAD"

    assert payload["result_readable"] is True
    assert payload["result"]["result"] == "success"
    assert payload["result"]["input"]["prompt"] == "Hello IRONCLAD"
    assert payload["result"]["output_text"] == "FAKE_RESPONSE: Hello IRONCLAD..."

    assert payload["tenant_submit_used"] is True
    assert payload["canonical_enqueue_used"] is True
    assert payload["production_lua_evalsha_path_used"] is True
    assert payload["scheduler_lua_python_fallback_used"] is False
    assert payload["worker_stream_consume_loop_used"] is True
    assert payload["run_requested_event_consumed"] is True
    assert payload["direct_process_message_call_used"] is False

    assert payload["state_store_result_written"] is True
    assert payload["state_store_mark_completed_called"] is True
    assert payload["message_acknowledged"] is True

    assert payload["artifact_backed_safe_local_adapter_used"] is False
    assert payload["guarded_real_executor_runtime_path_supported"] is True
    assert payload["guarded_real_executor_requested"] is False
    assert payload["provider_guard_required"] is True
    assert payload["provider_guard_status"] == "BLOCKED"
    assert payload["provider_guard_ready"] is False
    assert payload["real_executor_boundary_reachable"] is False
    assert payload["product_runtime_real_executor_execute_supported"] is True
    assert payload["product_runtime_real_executor_execute_requested"] is False
    assert payload["product_runtime_real_executor_execution_attempted"] is False
    assert payload["product_runtime_real_executor_executed"] is False
    assert payload["product_runtime_real_executor_execute_status"] == "BLOCKED"
    assert payload["product_runtime_real_executor_execute"]["network_call_attempted"] is False
    assert payload["product_runtime_real_executor_execute"]["production_llm_call_attempted"] is False
    assert payload["product_runtime_real_executor_execute"]["api_key_value_exposed"] is False
    assert payload["product_runtime_real_executor_execute"]["prompt_value_exposed"] is False
    assert payload["product_runtime_real_executor_execute"]["output_text_value_exposed"] is False
    assert payload["real_executor_boundary"]["network_call_attempted"] is False
    assert payload["real_executor_boundary"]["production_llm_call_attempted"] is False
    assert payload["real_executor_boundary"]["api_key_value_exposed"] is False
    assert payload["real_executor_boundary"]["prompt_value_exposed"] is False
    assert payload["real_executor_boundary"]["output_text_value_exposed"] is False
    assert payload["production_llm_call_attempted"] is False
    assert payload["deployment_attempted"] is False
    assert payload["release_tag_created"] is False
    assert payload["operator_action_buttons"] is False
    assert payload["noncanonical_redis_mutation_attempted"] is False

    assert ARTIFACT_PATH.exists()

    written = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
    assert written["status"] == "PASS"
    assert written["result_readable"] is True
    assert written["result"]["output_text"] == "FAKE_RESPONSE: Hello IRONCLAD..."
    assert written["guarded_real_executor_runtime_path_supported"] is True
    assert written["guarded_real_executor_requested"] is False
    assert written["provider_guard_required"] is True
    assert written["provider_guard_ready"] is False
    assert written["real_executor_boundary_reachable"] is False
    assert written["product_runtime_real_executor_execute_supported"] is True
    assert written["product_runtime_real_executor_execute_requested"] is False
    assert written["product_runtime_real_executor_execution_attempted"] is False
    assert written["product_runtime_real_executor_executed"] is False
    assert written["product_runtime_real_executor_execute_status"] == "BLOCKED"


def test_ironclad_result_cli_reads_latest_demo_result():
    if not ARTIFACT_PATH.exists():
        run_command(
            [
                "scripts/ironclad_demo.py",
                "--tenant",
                "demo",
                "--message",
                "Hello IRONCLAD",
                "--redis-url",
                REDIS_URL,
                "--json",
            ]
        )

    latest = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
    run_id = latest["run_id"]

    payload = run_command(
        [
            "scripts/ironclad_result.py",
            "--run-id",
            run_id,
            "--json",
        ]
    )

    assert payload["source"] == "ironclad_result"
    assert payload["status"] == "COMPLETED"
    assert payload["tenant_id"] == "tenant-demo"
    assert payload["task_id"]
    assert payload["run_id"] == run_id
    assert payload["result_readable"] is True
    assert payload["runtime_claim"] == "USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND"
    assert payload["uses_sprint_42_runtime"] is True

    assert payload["result"]["result"] == "success"
    assert payload["result"]["input"]["prompt"] == "Hello IRONCLAD"
    assert payload["result"]["output_text"] == "FAKE_RESPONSE: Hello IRONCLAD..."

    assert payload["production_llm_call_attempted"] is False
    assert payload["deployment_attempted"] is False
    assert payload["release_tag_created"] is False
    assert payload["operator_action_buttons"] is False


def test_ironclad_result_cli_returns_not_found_for_unknown_run_id():
    payload = run_command(
        [
            "scripts/ironclad_result.py",
            "--run-id",
            "run-does-not-exist",
            "--json",
        ]
    )

    assert payload["source"] == "ironclad_result"
    assert payload["status"] == "NOT_FOUND_OR_NOT_PERSISTED"
    assert payload["run_id"] == "run-does-not-exist"
    assert payload["result_readable"] is False
    assert payload["production_llm_call_attempted"] is False
    assert payload["deployment_attempted"] is False
    assert payload["release_tag_created"] is False
    assert payload["operator_action_buttons"] is False
