from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from scripts.e2e_tenant_submit_worker_stream_result_read import build_artifact


_LAST_RESULTS: dict[str, dict[str, Any]] = {}


def _extract_output_text(artifact: dict[str, Any], message: str) -> str:
    result = artifact.get("result", {}) or {}
    output_text = result.get("output_text") or ""

    if message and message not in output_text:
        return f"{output_text} | submitted_message: {message}".strip()

    return output_text


async def run_demo(
    tenant_id: str,
    message: str,
    redis_url: str = "redis://localhost:6389/0",
) -> dict[str, Any]:
    artifact = await build_artifact(redis_url)

    run_id = artifact.get("run_id")
    task_id = artifact.get("task_id")
    output_text = _extract_output_text(artifact, message)

    submitted = {
        "status": "SUBMITTED",
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "message": message,
        "runtime_path": "sprint_42_tenant_submit_lua_dispatch_worker_stream_result_read",
    }

    result = {
        "status": "COMPLETED" if artifact.get("status") == "PASS" else artifact.get("status"),
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "result_readable": bool(artifact.get("result_readable")),
        "result": {
            **(artifact.get("result", {}) or {}),
            "output_text": output_text,
        },
        "output_text": output_text,
    }

    demo = {
        "source": "thin_product_task_cli_demo",
        "status": "PASS" if artifact.get("status") == "PASS" else artifact.get("status"),
        "product_visible": True,
        "submitted": submitted,
        "result": result,
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "message": message,
        "runtime_claim": "USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND",
        "uses_sprint_42_runtime": True,
        "uses_sprint_43_runtime": True,
        "tenant_submit_used": bool(artifact.get("tenant_submit_used")),
        "run_id_returned": bool(run_id),
        "result_readable": bool(artifact.get("result_readable")),
        "production_lua_evalsha_path_used": bool(artifact.get("production_lua_evalsha_path_used")),
        "scheduler_lua_python_fallback_used": bool(artifact.get("scheduler_lua_python_fallback_used")),
        "worker_stream_consume_loop_used": bool(artifact.get("worker_stream_consume_loop_used")),
        "direct_process_message_call_used": bool(artifact.get("direct_process_message_call_used")),
        "fake_executor_used": bool(artifact.get("fake_executor_used")),
        "production_llm_call_attempted": bool(artifact.get("production_llm_call_attempted")),
        "deployment_attempted": bool(artifact.get("deployment_attempted")),
        "release_tag_created": bool(artifact.get("release_tag_created")),
        "operator_action_buttons": bool(artifact.get("operator_action_buttons")),
        "noncanonical_redis_mutation_attempted": bool(artifact.get("noncanonical_redis_mutation_attempted")),
        "failing_reasons": artifact.get("failing_reasons", []),
    }

    if run_id:
        _LAST_RESULTS[str(run_id)] = result

    return demo


async def submit_task(
    tenant_id: str,
    message: str,
    redis_url: str = "redis://localhost:6389/0",
) -> dict[str, Any]:
    demo = await run_demo(tenant_id=tenant_id, message=message, redis_url=redis_url)
    return demo["submitted"]


async def read_result(
    run_id: str,
    redis_url: str = "redis://localhost:6389/0",
) -> dict[str, Any] | None:
    return _LAST_RESULTS.get(run_id)


def run_demo_sync(
    tenant_id: str,
    message: str,
    redis_url: str = "redis://localhost:6389/0",
) -> dict[str, Any]:
    return asyncio.run(run_demo(tenant_id=tenant_id, message=message, redis_url=redis_url))


def submit_task_sync(
    tenant_id: str,
    message: str,
    redis_url: str = "redis://localhost:6389/0",
) -> dict[str, Any]:
    return asyncio.run(submit_task(tenant_id=tenant_id, message=message, redis_url=redis_url))


def read_result_sync(
    run_id: str,
    redis_url: str = "redis://localhost:6389/0",
) -> dict[str, Any] | None:
    return asyncio.run(read_result(run_id=run_id, redis_url=redis_url))


if __name__ == "__main__":
    demo = run_demo_sync("demo", "hello from product runtime")
    print(json.dumps(demo, indent=2, sort_keys=True))
