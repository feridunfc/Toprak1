from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Mapping

from scripts.ironclad_executor_mode import apply_executor_mode_boundary
from scripts.ironclad_manual_provider_smoke_guard import build_manual_provider_smoke_guard_artifact


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from scripts.e2e_tenant_submit_worker_stream_result_read import build_artifact


_LAST_RESULTS: dict[str, dict[str, Any]] = {}


def _extract_output_text(artifact: dict[str, Any], message: str) -> str:
    result = artifact.get("result", {}) or {}
    return result.get("output_text") or ""



def build_product_runtime_guarded_real_executor_boundary(
    env: Mapping[str, str] | None = None,
    *,
    message: str = "",
    executor_builder: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    source = env if env is not None else os.environ
    requested = (source.get("IRONCLAD_PRODUCT_RUNTIME_REAL_EXECUTOR") or "").strip() == "1"
    provider_guard = build_manual_provider_smoke_guard_artifact(dict(source))

    artifact: dict[str, Any] = {
        "source": "product_runtime_guarded_real_executor_boundary",
        "status": "BLOCKED",
        "guarded_real_executor_runtime_path_supported": True,
        "guarded_real_executor_requested": requested,
        "provider_guard_required": True,
        "provider_guard_status": provider_guard.get("status"),
        "provider_guard_ready": bool(provider_guard.get("manual_provider_smoke_ready")),
        "provider_allowed": bool(provider_guard.get("provider_allowed")),
        "model_allowed": bool(provider_guard.get("model_allowed")),
        "budget_guard_passed": bool(provider_guard.get("budget_guard_passed")),
        "operator_confirmed": bool(provider_guard.get("operator_confirmed")),
        "executor_factory_attempted": False,
        "real_executor_boundary_reachable": False,
        "executor_type": None,
        "provider": provider_guard.get("provider", "openai"),
        "model": provider_guard.get("model"),
        "network_call_attempted": False,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "prompt_length": len(message or ""),
        "prompt_value_exposed": False,
        "output_text_present": False,
        "output_text_value_exposed": False,
        "api_key_value_exposed": False,
        "failing_reasons": [],
    }

    if not requested:
        artifact["blocked_reason"] = "IRONCLAD_PRODUCT_RUNTIME_REAL_EXECUTOR=1 is required"
        return artifact

    if not artifact["provider_guard_ready"]:
        artifact["blocked_reason"] = (
            "manual provider smoke guard is not READY: "
            + str(provider_guard.get("blocked_reason") or "unknown guard block")
        )
        return artifact

    config = {
        "executor_mode": "openai",
        "openai_api_key": source.get("OPENAI_API_KEY"),
        "openai_model": source.get("OPENAI_MODEL", "gpt-4o-mini"),
        "executor_timeout_seconds": float(source.get("IRONCLAD_REAL_SMOKE_TIMEOUT_SECONDS", "30")),
    }

    artifact["executor_factory_attempted"] = True

    try:
        if executor_builder is None:
            from hfa_worker.executor_factory import build_executor

            executor = build_executor(config)
        else:
            executor = executor_builder(config)
    except Exception as exc:
        artifact["status"] = "FAILED"
        artifact["error_type"] = type(exc).__name__
        artifact["error_message_exposed"] = False
        artifact["failing_reasons"] = ["guarded real executor factory failed"]
        return artifact

    artifact["status"] = "READY"
    artifact["blocked_reason"] = "guarded real executor boundary reachable; execution remains explicit/manual"
    artifact["real_executor_boundary_reachable"] = True
    artifact["executor_type"] = type(executor).__name__
    return artifact



async def run_demo(
    tenant_id: str,
    message: str,
    redis_url: str = "redis://localhost:6389/0",
) -> dict[str, Any]:
    runtime_payload = {"prompt": message}
    artifact = await build_artifact(redis_url, payload=runtime_payload)

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

    real_executor_boundary = build_product_runtime_guarded_real_executor_boundary(
        os.environ,
        message=message,
    )

    demo["guarded_real_executor_runtime_path_supported"] = bool(
        real_executor_boundary.get("guarded_real_executor_runtime_path_supported")
    )
    demo["guarded_real_executor_requested"] = bool(
        real_executor_boundary.get("guarded_real_executor_requested")
    )
    demo["provider_guard_required"] = bool(real_executor_boundary.get("provider_guard_required"))
    demo["provider_guard_status"] = real_executor_boundary.get("provider_guard_status")
    demo["provider_guard_ready"] = bool(real_executor_boundary.get("provider_guard_ready"))
    demo["real_executor_boundary_reachable"] = bool(
        real_executor_boundary.get("real_executor_boundary_reachable")
    )
    demo["real_executor_boundary_status"] = real_executor_boundary.get("status")
    demo["real_executor_boundary"] = real_executor_boundary

    if run_id:
        _LAST_RESULTS[str(run_id)] = result

    return apply_executor_mode_boundary(demo)


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


