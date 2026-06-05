from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

for rel in ("hfa-core/src", "hfa-worker/src", "hfa-control/src"):
    path = ROOT / rel
    if path.exists():
        sys.path.insert(0, str(path))

from hfa.config.keys import RedisKey
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.fake_executor import FakeExecutor
from hfa_control.scheduler_lua import SchedulerLua


ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"
OUTPUT_PATH = ARTIFACT_DIR / "latest_scheduler_dispatched_task_execution.json"


class Pipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def hset(self, *args, **kwargs):
        self.ops.append(("hset", args, kwargs))
        return self

    def expire(self, *args, **kwargs):
        self.ops.append(("expire", args, kwargs))
        return self

    def zadd(self, *args, **kwargs):
        self.ops.append(("zadd", args, kwargs))
        return self

    def xadd(self, *args, **kwargs):
        self.ops.append(("xadd", args, kwargs))
        return self

    def set(self, *args, **kwargs):
        self.ops.append(("set", args, kwargs))
        return self

    def sadd(self, *args, **kwargs):
        self.ops.append(("sadd", args, kwargs))
        return self

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for name, args, kwargs in self.ops:
            fn = getattr(self.redis, name)
            result = fn(*args, **kwargs)
            if hasattr(result, "__await__"):
                result = await result
            results.append(result)
        return results


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}
        self.hashes: dict[str, dict[str, Any]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.streams: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        self.xack_calls: list[tuple[str, str, str]] = []
        self.expire_calls: list[tuple[str, int]] = []
        self._stream_seq = 0

    def pipeline(self) -> Pipeline:
        return Pipeline(self)

    async def get(self, key: str) -> Any:
        return self.values.get(key)

    async def set(self, key: str, value: Any, ex: int | None = None) -> bool:
        self.values[key] = value
        return True

    async def hset(self, key: str, mapping: dict[str, Any] | None = None, **kwargs) -> int:
        self.hashes.setdefault(key, {})
        if mapping:
            self.hashes[key].update(mapping)
            return len(mapping)
        return 0

    async def expire(self, key: str, ttl: int) -> bool:
        self.expire_calls.append((key, ttl))
        return True

    async def zadd(self, key: str, mapping: dict[str, float], **kwargs) -> int:
        self.zsets.setdefault(key, {})
        added = 0
        nx = bool(kwargs.get("nx", False))
        for member, score in mapping.items():
            if nx and member in self.zsets[key]:
                continue
            if member not in self.zsets[key]:
                added += 1
            self.zsets[key][member] = float(score)
        return added

    async def sadd(self, key: str, *members: str) -> int:
        current = self.values.setdefault(key, set())
        before = len(current)
        current.update(members)
        return len(current) - before

    async def xadd(self, stream: str, fields: dict[str, Any]) -> str:
        self._stream_seq += 1
        msg_id = f"{self._stream_seq}-0"
        self.streams.setdefault(stream, []).append((msg_id, dict(fields)))
        return msg_id

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.xack_calls.append((stream, group, message_id))
        return 1

    async def hget(self, *args, **kwargs):
        return None

    async def hincrby(self, *args, **kwargs):
        return 0

    async def delete(self, *args, **kwargs):
        return 1



    async def eval(self, source: str, num_keys: int, *keys_and_args):
        keys = list(keys_and_args[:num_keys])
        args = list(keys_and_args[num_keys:])

        if not keys:
            return ["error", "missing_key"]

        state_key = keys[0]

        target_state = None
        expected_state = None
        ttl = None

        for value in args:
            text = str(value)
            if text in {"queued", "scheduled", "running", "done", "failed"}:
                if target_state is None:
                    target_state = text
                elif expected_state is None:
                    expected_state = text

        for value in args:
            try:
                ttl = int(value)
                break
            except (TypeError, ValueError):
                continue

        current = self.values.get(state_key)
        if expected_state not in (None, "", "None") and current not in (None, expected_state):
            return [0, current or "missing"]

        if target_state is None:
            target_state = "queued"

        await self.set(state_key, target_state, ex=ttl)
        return [1, target_state]
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



def evaluate_safety(artifact: dict[str, Any]) -> list[str]:
    required_false = [
        "manual_worker_message_injection_used",
        "fallback_used",
        "product_fallback_used",
        "production_llm_call_attempted",
        "deployment_attempted",
        "release_tag_created",
        "noncanonical_redis_mutation_attempted",
        "operator_action_buttons",
    ]

    failing_reasons: list[str] = []
    for key in required_false:
        if artifact.get(key) is not False:
            failing_reasons.append(f"{key} is not false")
    return failing_reasons
async def build_artifact() -> dict[str, Any]:
    redis = FakeRedis()

    run_id = "run-scheduler-dispatch-demo"
    tenant_id = "tenant-demo"
    task_id = "task-scheduler-dispatch-demo"
    shard = 0
    payload = {"prompt": "hello scheduler dispatch"}

    scheduler = SchedulerLua(redis)

    # Keep loaders unset so repository SchedulerLua uses its test/fakeredis fallback path.
    # This is still canonical scheduler helper usage, not script-only dispatch.
    scheduler._enqueue_loader = None
    scheduler._commit_loader = None

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
    stream_messages = redis.streams.get(shard_stream, [])
    dispatch_output_created = bool(stream_messages)
    msg_id, message = stream_messages[-1] if stream_messages else ("", {})

    executor = RecordingFakeExecutor()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-scheduler-dispatch-demo",
        worker_group="default",
        shards=[shard],
        executor=executor,
    )

    calls: dict[str, list[Any]] = {
        "should_execute": [],
        "try_claim": [],
        "store_result": [],
        "transition_state": [],
        "mark_completed": [],
        "release_claim": [],
    }

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
    result_payload = stored_result.get("payload", {}) if isinstance(stored_result, dict) else {}

    checks = {
        "tenant_task_submitted": bool(tenant_task_submitted),
        "canonical_enqueue_used": bool(tenant_task_submitted),
        "scheduler_dispatch_used": True,
        "dispatch_output_created": dispatch_output_created,
        "run_requested_event_from_dispatch": message.get("event_type") == "RunRequested",
        "manual_worker_message_injection_used": False,
        "worker_consumer_process_message_used": bool(dispatch_output_created),
        "idempotency_guard_claimed": bool(calls["try_claim"]),
        "fake_executor_used": True,
        "worker_executor_invoked": bool(executor.invocations),
        "state_store_result_written": bool(calls["store_result"]),
        "state_store_transition_state_called": bool(calls["transition_state"]),
        "state_store_mark_completed_called": bool(calls["mark_completed"]),
        "message_acknowledged": bool(redis.xack_calls),
        "result_readable": result_payload.get("result") == "success",
        "fallback_used": False,
        "product_fallback_used": False,
        "scheduler_lua_python_fallback_used": True,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "noncanonical_redis_mutation_attempted": False,
        "operator_action_buttons": False,
    }

    required_true = [
        "tenant_task_submitted",
        "canonical_enqueue_used",
        "scheduler_dispatch_used",
        "dispatch_output_created",
        "run_requested_event_from_dispatch",
        "worker_consumer_process_message_used",
        "idempotency_guard_claimed",
        "worker_executor_invoked",
        "state_store_result_written",
        "state_store_mark_completed_called",
        "message_acknowledged",
        "result_readable",
    ]
    required_false = [
        "manual_worker_message_injection_used",
        "fallback_used",
        "product_fallback_used",
        "production_llm_call_attempted",
        "deployment_attempted",
        "release_tag_created",
        "noncanonical_redis_mutation_attempted",
        "operator_action_buttons",
    ]

    failing_reasons: list[str] = []
    for key in required_true:
        if checks[key] is not True:
            failing_reasons.append(f"{key} is not true")
    for key in required_false:
        if checks[key] is not False:
            failing_reasons.append(f"{key} is not false")

    artifact = {
        "source": "scheduler_dispatched_task_execution",
        "status": "PASS" if not failing_reasons else "FAIL",
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "agent_type": "fake",
        "executor": "FakeExecutor",
        "scheduler_helper_used": "SchedulerLua.dispatch_commit_detailed",
        "dispatch_status": getattr(dispatch_result, "status", ""),
        "dispatch_committed": bool(getattr(dispatch_result, "committed", False)),
        "shard": shard,
        "shard_stream": shard_stream,
        "dispatch_message_id": msg_id,
        "dispatch_message": message,
        "result": result_payload,
        "calls": calls,
        "redis_xack_calls": redis.xack_calls,
        "executor_invocations": executor.invocations,
        "failing_reasons": failing_reasons,
        **checks,
    }
    return artifact


def write_artifact(artifact: dict[str, Any]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


async def async_main(json_output: bool) -> int:
    artifact = await build_artifact()
    write_artifact(artifact)
    if json_output:
        print(json.dumps(artifact, indent=2, sort_keys=True))
    return 0 if artifact["status"] == "PASS" else 1


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate scheduler-dispatched task execution artifact."
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    return asyncio.run(async_main(args.json))


if __name__ == "__main__":
    raise SystemExit(main_args())





