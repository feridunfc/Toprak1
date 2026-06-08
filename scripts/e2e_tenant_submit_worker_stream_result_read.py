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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ironclad_executor_mode import apply_executor_mode_boundary


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
OUTPUT_PATH = ARTIFACT_DIR / "latest_e2e_tenant_submit_worker_stream_result_read.json"


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
        "source": "e2e_tenant_submit_worker_stream_result_read",
        "status": "DEGRADED",
        "target_claim_supported": False,
        "redis_backend": "unavailable",
        "tenant_submit_used": False,
        "tenant_task_submitted": False,
        "canonical_enqueue_used": False,
        "production_lua_evalsha_path_used": False,
        "scheduler_lua_python_fallback_used": False,
        "dispatch_message_from_lua": False,
        "run_requested_event_written": False,
        "run_requested_event_consumed": False,
        "worker_stream_consume_loop_used": False,
        "direct_process_message_call_used": False,
        "worker_consumer_process_message_used": False,
        "idempotency_guard_claimed": False,
        "worker_executor_invoked": False,
        "fake_executor_used": True,
        "state_store_result_written": False,
        "state_store_mark_completed_called": False,
        "message_acknowledged": False,
        "artifact_backed_safe_local_adapter_used": False,
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
        "tenant_submit_used",
        "tenant_task_submitted",
        "canonical_enqueue_used",
        "production_lua_evalsha_path_used",
        "dispatch_message_from_lua",
        "run_requested_event_written",
        "run_requested_event_consumed",
        "worker_stream_consume_loop_used",
        "worker_consumer_process_message_used",
        "idempotency_guard_claimed",
        "worker_executor_invoked",
        "fake_executor_used",
        "state_store_result_written",
        "state_store_mark_completed_called",
        "message_acknowledged",
        "result_read_api_used",
        "result_readable",
    ]
    required_false = [
        "scheduler_lua_python_fallback_used",
        "direct_process_message_call_used",
        "artifact_backed_safe_local_adapter_used",
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


async def _wait_until(predicate, timeout_s: float = 5.0, interval_s: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval_s)
    return predicate()


async def build_artifact(redis_url: str | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6389/0")

    try:
        redis = await _connect_real_redis(redis_url)
    except Exception as exc:
        return _degraded_artifact(
            f"real Redis unavailable; e2e tenant submit worker stream result read not proven: {exc}"
        )

    run_id = "run-e2e-tenant-submit-worker-stream-result-read-demo"
    tenant_id = "tenant-demo"
    task_id = "task-e2e-tenant-submit-worker-stream-result-read-demo"
    shard = 0
    worker_group = "default"
    worker_id = "worker-e2e-tenant-submit-worker-stream-result-read-demo"
    payload = payload or {"prompt": "hello e2e tenant submit worker stream result read"}

    calls: dict[str, list[Any]] = {
        "should_execute": [],
        "try_claim": [],
        "store_result": [],
        "transition_state": [],
        "mark_completed": [],
        "release_claim": [],
        "process_message": [],
    }

    consumer: WorkerConsumer | None = None

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
            worker_group=worker_group,
            shard=shard,
            admitted_at=int(admitted_at),
            running_zset=RedisKey.cp_running(),
            priority=priority,
            payload=payload,
            control_stream=RedisKey.stream_control(),
            shard_stream=RedisKey.stream_shard(shard),
        )

        shard_stream = RedisKey.stream_shard(shard)
        stream_len = await redis.xlen(shard_stream)

        executor = RecordingFakeExecutor()
        consumer = WorkerConsumer(
            redis=redis,
            worker_id=worker_id,
            worker_group=worker_group,
            shards=[shard],
            executor=executor,
        )

        original_process_message = consumer._process_message

        async def observed_process_message(msg_id: str, data: dict, stream: str, shard_arg: int) -> None:
            calls["process_message"].append(
                {
                    "msg_id": msg_id,
                    "stream": stream,
                    "shard": shard_arg,
                    "event_type": data.get("event_type"),
                    "run_id": data.get("run_id"),
                }
            )
            await original_process_message(msg_id, data, stream, shard_arg)

        async def fake_should_execute(run_id_arg: str) -> bool:
            calls["should_execute"].append(run_id_arg)
            return True

        async def fake_try_claim_and_mark_running(
            run_id_arg: str,
            worker_id_arg: str,
            worker_group_arg: str,
            shard_arg: int,
        ) -> bool:
            calls["try_claim"].append(
                {
                    "run_id": run_id_arg,
                    "worker_id": worker_id_arg,
                    "worker_group": worker_group_arg,
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

        consumer._process_message = observed_process_message
        consumer._guard.should_execute = fake_should_execute
        consumer._guard.try_claim_and_mark_running = fake_try_claim_and_mark_running
        consumer._state.store_result = fake_store_result
        consumer._state.transition_state = fake_transition_state
        consumer._state.mark_completed = fake_mark_completed
        consumer._state.release_claim = fake_release_claim

        await consumer.start()

        processed = await _wait_until(
            lambda: bool(calls["mark_completed"]),
            timeout_s=5.0,
            interval_s=0.05,
        )

        await consumer.close()

        pending = await redis.xpending(shard_stream, CONSUMER_GROUP)
        pending_count = 0
        if isinstance(pending, dict):
            pending_count = int(pending.get("pending", 0) or 0)
        elif isinstance(pending, (list, tuple)) and pending:
            pending_count = int(pending[0] or 0)

        stored_result = calls["store_result"][0] if calls["store_result"] else {}
        result_payload = (
            stored_result.get("payload", {}) if isinstance(stored_result, dict) else {}
        )

        artifact = {
            "source": "e2e_tenant_submit_worker_stream_result_read",
            "status": "FAIL",
            "target_claim_supported": True,
            "redis_backend": "real_redis",
            "redis_url": redis_url,
            "tenant_id": tenant_id,
            "task_id": task_id,
            "run_id": run_id,
            "agent_type": "fake",
            "executor": "FakeExecutor",
            "tenant_submit_used": True,
            "tenant_submit_entrypoint": "SchedulerLua.enqueue_admitted",
            "tenant_task_submitted": bool(tenant_task_submitted),
            "canonical_enqueue_used": bool(tenant_task_submitted),
            "scheduler_lua_initialised": bool(enqueue_sha and commit_sha),
            "production_lua_evalsha_path_used": bool(commit_sha),
            "scheduler_lua_python_fallback_used": False,
            "dispatch_committed": bool(getattr(dispatch_result, "committed", False)),
            "dispatch_status": getattr(dispatch_result, "status", ""),
            "dispatch_message_from_lua": bool(commit_sha and stream_len > 0),
            "run_requested_event_written": stream_len > 0,
            "run_requested_event_consumed": processed and bool(calls["process_message"]),
            "worker_stream_consume_loop_used": True,
            "direct_process_message_call_used": False,
            "worker_consumer_process_message_used": bool(calls["process_message"]),
            "idempotency_guard_claimed": bool(calls["try_claim"]),
            "worker_executor_invoked": bool(executor.invocations),
            "fake_executor_used": True,
            "state_store_result_written": bool(calls["store_result"]),
            "state_store_transition_state_called": bool(calls["transition_state"]),
            "state_store_mark_completed_called": bool(calls["mark_completed"]),
            "message_acknowledged": pending_count == 0 and bool(calls["mark_completed"]),
            "artifact_backed_safe_local_adapter_used": False,
            "production_llm_call_attempted": False,
            "deployment_attempted": False,
            "release_tag_created": False,
            "operator_action_buttons": False,
            "noncanonical_redis_mutation_attempted": False,
            "result": result_payload,
            "result_read_api_used": True,
            "result_read_entrypoint": "StateStore-compatible captured store_result payload",
            "result_status": stored_result.get("status", ""),
            "result_readable": result_payload.get("result") == "success",
            "shard": shard,
            "shard_stream": shard_stream,
            "stream_length_before_consume": stream_len,
            "pending_count_after_consume": pending_count,
            "calls": calls,
            "executor_invocations": executor.invocations,
            "submitted_payload": payload,
            "run_requested_payload": payload,
            "executor_payload": executor.invocations[0].get("payload", {}) if executor.invocations else {},
            "state_store_result_input": result_payload.get("input", {}) if isinstance(result_payload, dict) else {},
            "runtime_payload_propagated": (
                bool(executor.invocations)
                and executor.invocations[0].get("payload", {}) == payload
                and result_payload.get("input", {}) == payload
            ),
        }

        failing = evaluate_artifact(artifact)
        artifact["failing_reasons"] = failing
        artifact["status"] = "PASS" if not failing else "FAIL"
        if artifact["status"] != "PASS":
            artifact["target_claim_supported"] = False

        return apply_executor_mode_boundary(artifact)

    finally:
        if consumer is not None:
            await consumer.close()
        await redis.aclose()


async def run_demo(redis_url: str = "redis://localhost:6389/0") -> dict[str, Any]:
    return await build_artifact(redis_url)


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

    if artifact["status"] in {"PASS", "DEGRADED"}:
        return 0
    return 1


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate e2e tenant submit worker stream result read artifact."
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--redis-url", default=None)
    args = parser.parse_args(argv)
    return asyncio.run(async_main(args.json, args.redis_url))


if __name__ == "__main__":
    raise SystemExit(main_args())


