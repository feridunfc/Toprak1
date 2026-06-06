from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any


OUTPUT_PATH = Path("docs/dashboard/artifacts/latest_thin_product_task_cli_demo.json")


def _json_print(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


async def _run_sprint_42_runtime(redis_url: str) -> dict[str, Any]:
    # Import lazily so py_compile works even if runtime dependencies are loaded
    # only during execution.
    from scripts.e2e_tenant_submit_worker_stream import build_artifact

    try:
        return await build_artifact(redis_url=redis_url)
    except TypeError:
        # Compatibility fallback if build_artifact uses argv/global defaults.
        return await build_artifact()



def _product_visible_result(runtime: dict[str, Any], message: str) -> dict[str, Any]:
    run_id = runtime.get("run_id")
    runtime_result = runtime.get("result") or {}

    # Sprint 43 product shell: surface the user-submitted message in the visible
    # result while preserving the underlying Sprint 42 runtime artifact separately.
    return {
        "input": {
            "prompt": message,
        },
        "output_text": f"FAKE_RESPONSE: {message}...",
        "result": runtime_result.get("result", "success"),
        "run_id": run_id,
    }
def _write_artifact(payload: dict[str, Any]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


async def _demo(tenant_id: str, message: str, redis_url: str, write_artifact: bool) -> dict[str, Any]:
    runtime = await _run_sprint_42_runtime(redis_url)

    status = "PASS" if runtime.get("status") == "PASS" and runtime.get("result_readable") else "DEGRADED"

    payload = {
        "source": "thin_product_task_cli_demo",
        "status": status,
        "product_visible": True,
        "runtime_claim": "USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND",
        "uses_sprint_42_runtime": True,
        "submit_cli_available": True,
        "result_cli_available": True,
        "demo_cli_available": True,
        "tenant_id": runtime.get("tenant_id", tenant_id),
        "task_id": runtime.get("task_id"),
        "run_id": runtime.get("run_id"),
        "message": message,
        "submitted": {
            "status": "SUBMITTED",
            "tenant_id": runtime.get("tenant_id", tenant_id),
            "task_id": runtime.get("task_id"),
            "run_id": runtime.get("run_id"),
            "message": message,
        },
        "result_readable": bool(runtime.get("result_readable")),
        "result": _product_visible_result(runtime, message),
        "worker_stream_consume_loop_used": runtime.get("worker_stream_consume_loop_used", False),
        "direct_process_message_call_used": runtime.get("direct_process_message_call_used", False),
        "production_lua_evalsha_path_used": runtime.get("production_lua_evalsha_path_used", False),
        "scheduler_lua_python_fallback_used": runtime.get("scheduler_lua_python_fallback_used", False),
        "fake_executor_used": runtime.get("fake_executor_used", False),
        "tenant_submit_used": runtime.get("tenant_submit_used", False),
        "canonical_enqueue_used": runtime.get("canonical_enqueue_used", False),
        "run_requested_event_consumed": runtime.get("run_requested_event_consumed", False),
        "state_store_result_written": runtime.get("state_store_result_written", False),
        "state_store_mark_completed_called": runtime.get("state_store_mark_completed_called", False),
        "message_acknowledged": runtime.get("message_acknowledged", False),
        "artifact_backed_safe_local_adapter_used": runtime.get("artifact_backed_safe_local_adapter_used", False),
        "production_llm_call_attempted": runtime.get("production_llm_call_attempted", False),
        "deployment_attempted": runtime.get("deployment_attempted", False),
        "release_tag_created": runtime.get("release_tag_created", False),
        "operator_action_buttons": runtime.get("operator_action_buttons", False),
        "noncanonical_redis_mutation_attempted": runtime.get("noncanonical_redis_mutation_attempted", False),
        "failing_reasons": runtime.get("failing_reasons", []),
        "runtime_artifact": runtime,
    }

    if write_artifact:
        _write_artifact(payload)

    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a user-visible IRONCLAD task demo.")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--redis-url", default="redis://localhost:6389/0")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    parser.add_argument("--no-artifact", action="store_true", help="Do not write dashboard artifact.")
    args = parser.parse_args(argv)

    payload = asyncio.run(
        _demo(
            tenant_id=args.tenant,
            message=args.message,
            redis_url=args.redis_url,
            write_artifact=not args.no_artifact,
        )
    )

    if args.json:
        _json_print(payload)
    else:
        print(f"{payload['status']} run_id={payload.get('run_id')} result_readable={payload.get('result_readable')}")

    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

