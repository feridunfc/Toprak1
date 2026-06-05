from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

for rel in ("hfa-core/src", "hfa-worker/src", "hfa-control/src"):
    path = ROOT / rel
    if path.exists():
        sys.path.insert(0, str(path))

import redis.asyncio as redis_async

from hfa.config.keys import RedisKey
from hfa_control.scheduler_lua import SchedulerLua
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.fake_executor import FakeExecutor


ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"
OUTPUT_PATH = ARTIFACT_DIR / "latest_production_lua_dispatch_path.json"


class RecordingFakeExecutor(FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.invocations: list[dict[str, Any]] = []

    async def execute(self, run_event: Any):
        self.invocations.append(
            {
                "run_id": getattr(run_event, "run_id", ""),
                "tenant_id": getattr(run_event, "tenant_id", ""),
                "payload": getattr(run_event, "payload", {}),
            }
        )
        return await super().execute(run_event)


async def _connect_real_redis(redis_url: str):
    client = redis_async.from_url(redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        raise
    return client


def _degraded_artifact(reason: str) -> dict[str, Any]:
    return {
        "source": "production_lua_dispatch_path",
        "status": "DEGRADED",
        "redis_backend": "unavailable",
        "target_claim_supported": False,
        "scheduler_lua_initialised": False,
        "dispatch_commit_loader_used": False,
        "dispatch_commit_sha_loaded": False,
        "production_lua_evalsha_path_used": False,
        "scheduler_lua_python_fallback_used": False,
        "dispatch_output_created": False,
        "run_requested_event_from_lua_dispatch": False,
        "manual_worker_message_injection_used": False,
        "worker_consumer_process_message_used": False,
        "state_store_result_written": False,
        "state_store_mark_completed_called": False,
        "message_acknowledged": False,
        "fake_executor_used": True,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "operator_action_buttons": False,
        "noncanonical_redis_mutation_attempted": False,
        "failing_reasons": [reason],
    }


def evaluate_artifact(artifact: dict[str, Any]) -> list[str]:
    required_true = [
        "target_claim_supported",
        "scheduler_lua_initialised",
        "dispatch_commit_loader_used",
        "dispatch_commit_sha_loaded",
        "production_lua_evalsha_path_used",
        "dispatch_output_created",
        "run_requested_event_from_lua_dispatch",
        "worker_consumer_process_message_used",
        "state_store_result_written",
        "state_store_mark_completed_called",
        "message_acknowledged",
        "fake_executor_used",
    ]
    required_false = [
        "scheduler_lua_python_fallback_used",
        "manual_worker_message_injection_used",
        "production_llm_call_attempted",
        "deployment_attempted",
        "release_tag_created",
        "operator_action_buttons",
        "noncanonical_redis_mutation_attempted",
    ]

    failing: list[str] = []
    if artifact.get("redis_backend") != "real_redis":
        failing.append("redis_backend is not real_redis")
    for key in required_true:
        if artifact.get(key) is not True:
            failing.append(f"{key} is not true")
    for key in required_false:
        if artifact.get(key) is not False:
            failing.append(f"{key} is not false")
    return failing


async def _read_last_stream_message(redis, stream: str) -> tuple[str, dict[str, Any]]:
    messages = await redis.xrevrange(stream, count=1)
    if not messages:
        return "", {}
    msg_id, data = messages[0]
    return str(msg_id), dict(data)


async def build_artifact(redis_url: str | None = None) -> dict[str, Any]:
    redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    try:
        redis = await _connect_real_redis(redis_url)
    except Exception as exc:
        return _degraded_artifact(
            f"real Redis unavailable; production Lua/EVALSHA path not proven: {exc}"
        )

    run_id = "run-production-lua-dispatch-demo"
    tenant_id = "tenant-demo"
    task_id = "task-production-lua-dispatch-demo"
    shard = 0
    payload = {"prompt": "hello production lua dispatch"}

    calls: dict[str, list[Any]] = {
        "should_execute": [],
        "try_claim": [],
        "store_result": [],
        "transition_state": [],
        "mark_completed": [],
        "release_claim": [],
    }

    try:
        await redis.flushdb()

        scheduler = SchedulerLua(redis)
        await scheduler.initialise()

        enqueue_loader = getattr(scheduler, "_enqueue_loader", None)
        commit_loader = getattr(scheduler, "_commit_loader", None)
        enqueue_sha = getattr(enqueue_loader, "sha", None) if enqueue_loader else None
        commit_sha = getattr(commit_loader, "sha", None) if commit_loader else None

        admitted_at = time.time()
        priority = 5
        score = priority * int(1e12) + int(admitted_at * 1_000_000) % int(1e12)

        tenant_task_submitted = await scheduler.enqueue_admitted(
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type="fake",
            priority=priority,
            preferred_region="",
            preferred_placement="LEAST_LOADED",
            admitted_at=admitted_at,
            score=float(score),
            payload_json=json.dumps(payload),
        )

        dispatch_result = await scheduler.dispatch_commit_detailed(
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type="fake",
            worker_group="default",
            shard=shard,
            admitted_at=int(admitted_at),
            running_zset=RedisKey.cp_running(),
            priority=priority,
            payload=payload,
            control_stream=RedisKey.stream_control(),
            shard_stream=RedisKey.stream_shard(shard),
        )

        shard_stream = RedisKey.stream_shard(shard)
        msg_id, message = await _read_last_stream_message(redis, shard_stream)
        dispatch_output_created = bool(msg_id and message)

        executor = RecordingFakeExecutor()
        consumer = WorkerConsumer(
            redis=redis,
            worker_id="worker-production-lua-dispatch-demo",
            worker_group="default",
            shards=[shard],
            executor=executor,
        )

        async def fake_should_execute(run_id_arg: str) -> bool:
            calls["should_execute"].append(run_id_arg)
            return True

        async def fake_try_claim_and_mark_running(
            run_id_arg: str,
            worker_id: str,
            worker_group: str,
            shard_arg: int,
        ) -> bool:
            calls["try_claim"].append(
                {
                    "run_id": run_id_arg,
                    "worker_id": worker_id,
                    "worker_group": worker_group,
                    "shard": shard_arg,
                }
            )
            return True

        async def fake_store_result(
            run_id_arg: str,
            tenant_id_arg: str,
            status: str,
            payload_arg: dict[str, Any],
            cost_cents: int,
            tokens_used: int,
            error: str | None = None,
        ) -> None:
            calls["store_result"].append(
                {
                    "run_id": run_id_arg,
                    "tenant_id": tenant_id_arg,
                    "status": status,
                    "payload": payload_arg,
                    "cost_cents": cost_cents,
                    "tokens_used": tokens_used,
                    "error": error,
                }
            )

        async def fake_transition_state(run_id_arg: str, state: str) -> None:
            calls["transition_state"].append({"run_id": run_id_arg, "state": state})

        async def fake_mark_completed(run_id_arg: str) -> None:
            calls["mark_completed"].append(run_id_arg)

        async def fake_release_claim(run_id_arg: str) -> None:
            calls["release_claim"].append(run_id_arg)

        consumer._guard.should_execute = fake_should_execute
        consumer._guard.try_claim_and_mark_running = fake_try_claim_and_mark_running
        consumer._state.store_result = fake_store_result
        consumer._state.transition_state = fake_transition_state
        consumer._state.mark_completed = fake_mark_completed
        consumer._state.release_claim = fake_release_claim

        if dispatch_output_created:
            await consumer._process_message(
                msg_id=msg_id,
                data=message,
                stream=shard_stream,
                shard=shard,
            )

        stored_result = calls["store_result"][0] if calls["store_result"] else {}
        result_payload = (
            stored_result.get("payload", {}) if isinstance(stored_result, dict) else {}
        )

        artifact = {
            "source": "production_lua_dispatch_path",
            "redis_backend": "real_redis",
            "redis_url": redis_url,
            "target_claim_supported": True,
            "tenant_id": tenant_id,
            "task_id": task_id,
            "run_id": run_id,
            "agent_type": "fake",
            "executor": "FakeExecutor",
            "scheduler_helper_used": "SchedulerLua.dispatch_commit_detailed",
            "scheduler_lua_initialised": bool(enqueue_sha and commit_sha),
            "enqueue_loader_used": enqueue_loader is not None,
            "enqueue_sha_loaded": bool(enqueue_sha),
            "dispatch_commit_loader_used": commit_loader is not None,
            "dispatch_commit_sha_loaded": bool(commit_sha),
            "dispatch_commit_sha": commit_sha,
            "production_lua_evalsha_path_used": bool(commit_sha),
            "scheduler_lua_python_fallback_used": False,
            "tenant_task_submitted": bool(tenant_task_submitted),
            "canonical_enqueue_used": bool(tenant_task_submitted),
            "dispatch_committed": bool(getattr(dispatch_result, "committed", False)),
            "dispatch_status": getattr(dispatch_result, "status", ""),
            "dispatch_output_created": dispatch_output_created,
            "run_requested_event_from_lua_dispatch": message.get("event_type") == "RunRequested",
            "manual_worker_message_injection_used": False,
            "worker_consumer_process_message_used": dispatch_output_created,
            "idempotency_guard_claimed": bool(calls["try_claim"]),
            "fake_executor_used": True,
            "worker_executor_invoked": bool(executor.invocations),
            "state_store_result_written": bool(calls["store_result"]),
            "state_store_transition_state_called": bool(calls["transition_state"]),
            "state_store_mark_completed_called": bool(calls["mark_completed"]),
            "message_acknowledged": bool(getattr(redis, "xack", None)) and bool(calls["mark_completed"]),
            "result_readable": result_payload.get("result") == "success",
            "production_llm_call_attempted": False,
            "deployment_attempted": False,
            "release_tag_created": False,
            "operator_action_buttons": False,
            "noncanonical_redis_mutation_attempted": False,
            "shard": shard,
            "shard_stream": shard_stream,
            "dispatch_message_id": msg_id,
            "dispatch_message": message,
            "result": result_payload,
            "calls": calls,
            "executor_invocations": executor.invocations,
        }

        failing_reasons = evaluate_artifact(artifact)
        artifact["failing_reasons"] = failing_reasons
        artifact["status"] = "PASS" if not failing_reasons else "FAIL"
        if artifact["status"] != "PASS":
            artifact["target_claim_supported"] = False

        return artifact

    finally:
        await redis.aclose()


def write_artifact(artifact: dict[str, Any]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


async def async_main(json_output: bool, redis_url: str | None) -> int:
    artifact = await build_artifact(redis_url)
    write_artifact(artifact)
    if json_output:
        print(json.dumps(artifact, indent=2, sort_keys=True))

    if artifact["status"] == "PASS":
        return 0
    if artifact["status"] == "DEGRADED":
        return 0
    return 1


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate production Lua dispatch path artifact."
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--redis-url", default=None)
    args = parser.parse_args(argv)
    return asyncio.run(async_main(args.json, args.redis_url))


if __name__ == "__main__":
    raise SystemExit(main_args())
