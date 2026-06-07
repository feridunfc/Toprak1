from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import redis.asyncio as redis_async

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
except Exception as exc:
    raise RuntimeError(
        "Missing API dependencies. Install with: python -m pip install fastapi uvicorn httpx"
    ) from exc

from scripts.ironclad_product_runtime import read_result, run_demo


ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"
OUTPUT_PATH = ARTIFACT_DIR / "latest_minimal_product_task_http_api.json"

RUN_CACHE: dict[str, dict[str, Any]] = {}


class TaskRequest(BaseModel):
    tenant_id: str
    message: str


def _redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6389/0")


def _submitted_response(demo: dict[str, Any]) -> dict[str, Any]:
    submitted = demo.get("submitted", {}) or {}
    result = demo.get("result", {}) or {}

    return {
        "status": "SUBMITTED",
        "tenant_id": submitted.get("tenant_id"),
        "task_id": submitted.get("task_id"),
        "run_id": submitted.get("run_id"),
        "message": submitted.get("message"),
        "result_readable": bool(demo.get("result_readable")),
        "uses_sprint_43_runtime": bool(demo.get("uses_sprint_43_runtime")),
        "production_lua_evalsha_path_used": bool(demo.get("production_lua_evalsha_path_used")),
        "worker_stream_consume_loop_used": bool(demo.get("worker_stream_consume_loop_used")),
        "direct_process_message_call_used": bool(demo.get("direct_process_message_call_used")),
        "fake_executor_used": bool(demo.get("fake_executor_used")),
        "result": result,
    }


def _completed_response(demo: dict[str, Any]) -> dict[str, Any]:
    submitted = demo.get("submitted", {}) or {}
    result = demo.get("result", {}) or {}

    output_text = result.get("output_text") or result.get("result", {}).get("output_text") or ""

    return {
        "status": "COMPLETED",
        "tenant_id": submitted.get("tenant_id"),
        "task_id": submitted.get("task_id"),
        "run_id": submitted.get("run_id"),
        "result_readable": bool(demo.get("result_readable")),
        "result": {
            **result,
            "output_text": output_text,
        },
        "uses_sprint_43_runtime": bool(demo.get("uses_sprint_43_runtime")),
        "production_lua_evalsha_path_used": bool(demo.get("production_lua_evalsha_path_used")),
        "worker_stream_consume_loop_used": bool(demo.get("worker_stream_consume_loop_used")),
        "direct_process_message_call_used": bool(demo.get("direct_process_message_call_used")),
        "fake_executor_used": bool(demo.get("fake_executor_used")),
        "production_llm_call_attempted": bool(demo.get("production_llm_call_attempted")),
        "deployment_attempted": bool(demo.get("deployment_attempted")),
        "release_tag_created": bool(demo.get("release_tag_created")),
        "operator_action_buttons": bool(demo.get("operator_action_buttons")),
    }


def create_app(redis_url: str | None = None) -> FastAPI:
    app = FastAPI(title="IRONCLAD Minimal Product Task HTTP API")
    app.state.redis_url = redis_url or _redis_url()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "ironclad_minimal_product_task_api",
            "runtime": "sprint_43",
            "redis_url": app.state.redis_url,
        }

    @app.post("/tasks")
    async def post_tasks(request: TaskRequest) -> dict[str, Any]:
        demo = await run_demo(
            tenant_id=request.tenant_id,
            message=request.message,
            redis_url=app.state.redis_url,
        )

        submitted = _submitted_response(demo)
        completed = _completed_response(demo)

        run_id = submitted.get("run_id")
        if not run_id:
            raise HTTPException(status_code=500, detail="runtime did not return run_id")

        RUN_CACHE[str(run_id)] = completed
        return submitted

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        if run_id in RUN_CACHE:
            return RUN_CACHE[run_id]

        result = await read_result(run_id=run_id, redis_url=app.state.redis_url)
        if result:
            result.setdefault("status", "COMPLETED")
            result.setdefault("run_id", run_id)
            result.setdefault("result_readable", True)
            return result

        raise HTTPException(
            status_code=404,
            detail={
                "status": "NOT_FOUND_OR_NOT_PERSISTED",
                "run_id": run_id,
            },
        )

    @app.post("/demo")
    async def post_demo(request: TaskRequest) -> dict[str, Any]:
        demo = await run_demo(
            tenant_id=request.tenant_id,
            message=request.message,
            redis_url=app.state.redis_url,
        )
        completed = _completed_response(demo)

        run_id = completed.get("run_id")
        if run_id:
            RUN_CACHE[str(run_id)] = completed

        return {
            "source": "minimal_product_task_http_api_demo",
            "status": "COMPLETED",
            "product_visible": True,
            "submitted": {
                "tenant_id": completed.get("tenant_id"),
                "task_id": completed.get("task_id"),
                "run_id": completed.get("run_id"),
            },
            "result": completed.get("result", {}),
            "safety": {
                "production_llm_call_attempted": completed.get("production_llm_call_attempted", False),
                "deployment_attempted": completed.get("deployment_attempted", False),
                "release_tag_created": completed.get("release_tag_created", False),
                "operator_action_buttons": completed.get("operator_action_buttons", False),
            },
        }

    return app


app = create_app()


def _evaluate(artifact: dict[str, Any]) -> list[str]:
    required_true = [
        "http_api_available",
        "health_endpoint_available",
        "post_tasks_available",
        "get_run_result_available",
        "uses_sprint_43_runtime",
        "result_readable",
        "result_output_includes_submitted_message",
        "production_lua_evalsha_path_used",
        "worker_stream_consume_loop_used",
        "fake_executor_used",
    ]
    required_false = [
        "direct_process_message_call_used",
        "production_llm_call_attempted",
        "deployment_attempted",
        "release_tag_created",
        "operator_action_buttons",
    ]

    failing: list[str] = []
    for key in required_true:
        if artifact.get(key) is not True:
            failing.append(f"{key} is not true")
    for key in required_false:
        if artifact.get(key) is not False:
            failing.append(f"{key} is not false")
    return failing


async def _wait_for_redis(redis_url: str, timeout_s: float = 10.0) -> None:
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

    raise RuntimeError(f"Redis did not become ready for API self-test: {last_error!r}")


async def self_test(redis_url: str, tenant_id: str, message: str) -> dict[str, Any]:
    from httpx import ASGITransport, AsyncClient

    await _wait_for_redis(redis_url)

    test_app = create_app(redis_url)
    transport = ASGITransport(app=test_app)

    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        health_response = await client.get("/health")
        task_response = await client.post(
            "/tasks",
            json={"tenant_id": tenant_id, "message": message},
        )

        submitted = task_response.json()
        run_id = submitted.get("run_id")

        result_response = await client.get(f"/runs/{run_id}")
        result = result_response.json()

    output_text = result.get("result", {}).get("output_text") or ""

    artifact = {
        "source": "minimal_product_task_http_api",
        "status": "FAIL",
        "target_claim_supported": True,
        "http_api_available": True,
        "health_endpoint_available": health_response.status_code == 200 and health_response.json().get("status") == "ok",
        "post_tasks_available": task_response.status_code == 200,
        "get_run_result_available": result_response.status_code == 200,
        "uses_sprint_43_runtime": bool(submitted.get("uses_sprint_43_runtime")) and bool(result.get("uses_sprint_43_runtime")),
        "tenant_id": tenant_id,
        "run_id": run_id,
        "task_id": submitted.get("task_id"),
        "result_readable": bool(result.get("result_readable")),
        "result_output_includes_submitted_message": message in output_text,
        "production_lua_evalsha_path_used": bool(submitted.get("production_lua_evalsha_path_used") or result.get("production_lua_evalsha_path_used")),
        "worker_stream_consume_loop_used": bool(submitted.get("worker_stream_consume_loop_used") or result.get("worker_stream_consume_loop_used")),
        "direct_process_message_call_used": bool(submitted.get("direct_process_message_call_used") or result.get("direct_process_message_call_used")),
        "fake_executor_used": bool(submitted.get("fake_executor_used") or result.get("fake_executor_used")),
        "production_llm_call_attempted": bool(result.get("production_llm_call_attempted")),
        "deployment_attempted": bool(result.get("deployment_attempted")),
        "release_tag_created": bool(result.get("release_tag_created")),
        "operator_action_buttons": bool(result.get("operator_action_buttons")),
        "submitted": submitted,
        "result": result,
        "health": health_response.json(),
    }

    failing = _evaluate(artifact)
    artifact["failing_reasons"] = failing
    artifact["status"] = "PASS" if not failing else "FAIL"
    if artifact["status"] != "PASS":
        artifact["target_claim_supported"] = False

    return artifact


def write_artifact(artifact: dict[str, Any]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")


async def async_main(args: argparse.Namespace) -> int:
    if args.self_test:
        artifact = await self_test(args.redis_url, args.tenant, args.message)
        write_artifact(artifact)
        if args.json:
            print(json.dumps(artifact, indent=2, sort_keys=True))
        return 0 if artifact["status"] == "PASS" else 1

    import uvicorn

    os.environ["REDIS_URL"] = args.redis_url
    uvicorn.run(
        "scripts.ironclad_api:app",
        host=args.host,
        port=args.port,
        reload=False,
    )
    return 0


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run IRONCLAD minimal product task HTTP API.")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6389/0"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--tenant", default="demo")
    parser.add_argument("--message", default="hello from HTTP API")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    os.environ["REDIS_URL"] = args.redis_url
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main_args())

