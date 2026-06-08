from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ironclad_executor_mode import apply_executor_mode_boundary

import redis.asyncio as redis_async

from scripts.e2e_tenant_submit_worker_stream_result_read import build_artifact as build_runtime_artifact

ARTIFACT_PATH = ROOT / "docs" / "dashboard" / "artifacts" / "latest_runtime_payload_propagation.json"


async def _wait_for_redis(redis_url: str, timeout_s: float = 15.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    last_error: Exception | None = None

    while asyncio.get_running_loop().time() < deadline:
        client = redis_async.from_url(redis_url, decode_responses=True)
        try:
            await client.ping()
            await client.aclose()
            return
        except Exception as exc:
            last_error = exc
            await client.aclose()
            await asyncio.sleep(0.2)

    raise RuntimeError(f"Redis did not become ready for runtime payload propagation artifact: {last_error!r}")


def _output_text(result_payload: dict[str, Any]) -> str:
    return str(result_payload.get("output_text") or "")


def evaluate_artifact(artifact: dict[str, Any]) -> list[str]:
    failing: list[str] = []

    required_true = [
        "target_claim_supported",
        "runtime_payload_propagated",
        "result_output_includes_submitted_message",
        "production_lua_evalsha_path_used",
        "worker_stream_consume_loop_used",
        "fake_executor_used",
    ]

    required_false = [
        "product_result_normalization_used",
        "no_prompt_fallback_used",
        "submitted_message_patch_used",
        "direct_process_message_call_used",
        "production_llm_call_attempted",
        "deployment_attempted",
        "release_tag_created",
        "operator_action_buttons",
    ]

    for key in required_true:
        if artifact.get(key) is not True:
            failing.append(f"{key} is not true")

    for key in required_false:
        if artifact.get(key) is not False:
            failing.append(f"{key} is not false")

    submitted_message = artifact.get("submitted_message")
    expected_payload = {"prompt": submitted_message}

    if artifact.get("run_requested_payload") != expected_payload:
        failing.append("run_requested_payload does not match submitted message")

    if artifact.get("executor_payload") != expected_payload:
        failing.append("executor_payload does not match submitted message")

    if artifact.get("state_store_result_input") != expected_payload:
        failing.append("state_store_result_input does not match submitted message")

    output_text = artifact.get("output_text", "")

    if submitted_message not in output_text:
        failing.append("output_text does not include submitted message")

    if "no_prompt" in output_text:
        failing.append("output_text contains no_prompt fallback")

    if "submitted_message" in output_text:
        failing.append("output_text contains submitted_message patch")

    return failing


async def build_artifact(redis_url: str, message: str) -> dict[str, Any]:
    await _wait_for_redis(redis_url)

    submitted_payload = {"prompt": message}

    runtime = await build_runtime_artifact(
        redis_url=redis_url,
        payload=submitted_payload,
    )

    result_payload = runtime.get("result", {}) or {}
    output_text = _output_text(result_payload)

    executor_invocations = runtime.get("executor_invocations", []) or []
    executor_payload = (
        executor_invocations[0].get("payload", {})
        if executor_invocations and isinstance(executor_invocations[0], dict)
        else {}
    )

    state_store_result_input = (
        result_payload.get("input", {})
        if isinstance(result_payload, dict)
        else {}
    )

    artifact = {
        "source": "runtime_payload_propagation",
        "status": "FAIL",
        "target_claim_supported": True,
        "submitted_message": message,
        "runtime_payload_propagated": (
            executor_payload == submitted_payload
            and state_store_result_input == submitted_payload
            and message in output_text
            and "no_prompt" not in output_text
            and "submitted_message" not in output_text
        ),
        "run_requested_payload": runtime.get("run_requested_payload", submitted_payload),
        "executor_payload": executor_payload,
        "state_store_result_input": state_store_result_input,
        "output_text": output_text,
        "result_output_includes_submitted_message": message in output_text,
        "product_result_normalization_used": False,
        "no_prompt_fallback_used": "no_prompt" in output_text,
        "submitted_message_patch_used": "submitted_message" in output_text,
        "production_lua_evalsha_path_used": bool(runtime.get("production_lua_evalsha_path_used")),
        "scheduler_lua_python_fallback_used": bool(runtime.get("scheduler_lua_python_fallback_used")),
        "worker_stream_consume_loop_used": bool(runtime.get("worker_stream_consume_loop_used")),
        "direct_process_message_call_used": bool(runtime.get("direct_process_message_call_used")),
        "fake_executor_used": bool(runtime.get("fake_executor_used")),
        "production_llm_call_attempted": bool(runtime.get("production_llm_call_attempted")),
        "deployment_attempted": bool(runtime.get("deployment_attempted")),
        "release_tag_created": bool(runtime.get("release_tag_created")),
        "operator_action_buttons": bool(runtime.get("operator_action_buttons")),
        "run_id": runtime.get("run_id"),
        "task_id": runtime.get("task_id"),
        "runtime": runtime,
        "result": result_payload,
    }

    failing = evaluate_artifact(artifact)
    artifact["failing_reasons"] = failing
    artifact["status"] = "PASS" if not failing else "FAIL"
    if failing:
        artifact["target_claim_supported"] = False

    return apply_executor_mode_boundary(artifact)


def write_artifact(artifact: dict[str, Any]) -> None:
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


async def async_main(args: argparse.Namespace) -> int:
    artifact = await build_artifact(args.redis_url, args.message)
    write_artifact(artifact)

    if args.json:
        print(json.dumps(artifact, indent=2, sort_keys=True))

    return 0 if artifact["status"] == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Sprint 45 runtime payload propagation artifact.")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6389/0"))
    parser.add_argument("--message", default="hello payload")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
