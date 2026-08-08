from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

for rel in ("hfa-core/src", "hfa-worker/src", "hfa-control/src"):
    path = ROOT / rel
    if path.exists():
        sys.path.insert(0, str(path))

from hfa.config.keys import RedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.fake_executor import FakeExecutor


ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"
OUTPUT_PATH = ARTIFACT_DIR / "latest_canonical_worker_task_execution.json"


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


class FakePipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self._redis = redis
        self._commands: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def watch(self, *keys: str) -> None:
        return None

    async def unwatch(self) -> None:
        return None

    async def type(self, key: str) -> str:
        return "hash" if key in self._redis.hashes else "none"

    async def hmget(self, key: str, *fields: str):
        values = self._redis.hashes.get(key, {})
        return [values.get(field) for field in fields]

    async def ttl(self, key: str) -> int:
        return -1 if key in self._redis.hashes else -2

    async def hget(self, key: str, field: str):
        return self._redis.hashes.get(key, {}).get(field)

    def multi(self) -> None:
        self._commands.clear()

    def xadd(self, *args: Any, **kwargs: Any):
        self._commands.append(("xadd", args, kwargs))
        return self

    def hset(self, *args: Any, **kwargs: Any):
        self._commands.append(("hset", args, kwargs))
        return self

    def persist(self, *args: Any, **kwargs: Any):
        self._commands.append(("persist", args, kwargs))
        return self

    async def execute(self):
        results = []
        for operation, args, kwargs in self._commands:
            method = getattr(self._redis, operation)
            results.append(await method(*args, **kwargs))
        self._commands.clear()
        return results


class FakeRedis:
    def __init__(self) -> None:
        self.xack_calls: list[tuple[str, str, str]] = []
        self.xadd_calls: list[tuple[str, dict[str, Any]]] = []
        self.hashes: dict[str, dict[str, Any]] = {
            RedisKey.run_terminal_event_index(): {
                "__contract__:schema_version": "1",
                "__contract__:producer_contract_version": "1",
            }
        }

    def pipeline(self, *, transaction: bool = True) -> FakePipeline:
        return FakePipeline(self)

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.xack_calls.append((stream, group, message_id))
        return 1

    async def xadd(self, stream: str, fields: dict[str, Any]) -> str:
        self.xadd_calls.append((stream, fields))
        return "2-0"

    async def hget(self, key: str, field: str):
        return self.hashes.get(key, {}).get(field)

    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        *,
        mapping: dict[str, Any] | None = None,
    ) -> int:
        target = self.hashes.setdefault(key, {})
        if mapping is not None:
            target.update(mapping)
            return len(mapping)
        if field is not None:
            target[field] = value
            return 1
        return 0

    async def persist(self, key: str) -> bool:
        return key in self.hashes

    async def hincrby(self, *args, **kwargs):
        return 0

    async def delete(self, *args, **kwargs):
        return 1


async def build_artifact() -> dict[str, Any]:
    redis = FakeRedis()
    executor = RecordingFakeExecutor()

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-canonical-demo",
        worker_group="default",
        shards=[0],
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

    async def fake_should_execute(run_id: str) -> bool:
        calls["should_execute"].append(run_id)
        return True

    async def fake_try_claim_and_mark_running(
        run_id: str,
        worker_id: str,
        worker_group: str,
        shard: int,
    ) -> bool:
        calls["try_claim"].append(
            {
                "run_id": run_id,
                "worker_id": worker_id,
                "worker_group": worker_group,
                "shard": shard,
            }
        )
        return True

    async def fake_store_result(
        run_id: str,
        tenant_id: str,
        status: str,
        payload: dict[str, Any],
        cost_cents: int,
        tokens_used: int,
        error: str | None = None,
    ) -> None:
        calls["store_result"].append(
            {
                "run_id": run_id,
                "tenant_id": tenant_id,
                "status": status,
                "payload": payload,
                "cost_cents": cost_cents,
                "tokens_used": tokens_used,
                "error": error,
            }
        )

    async def fake_transition_state(run_id: str, state: str) -> None:
        calls["transition_state"].append({"run_id": run_id, "state": state})

    async def fake_mark_completed(run_id: str) -> None:
        calls["mark_completed"].append(run_id)

    async def fake_release_claim(run_id: str) -> None:
        calls["release_claim"].append(run_id)

    consumer._guard.should_execute = fake_should_execute
    consumer._guard.try_claim_and_mark_running = fake_try_claim_and_mark_running
    consumer._state.store_result = fake_store_result
    consumer._state.transition_state = fake_transition_state
    consumer._state.mark_completed = fake_mark_completed
    consumer._state.release_claim = fake_release_claim

    event = RunRequestedEvent(
        run_id="run-canonical-worker-demo",
        tenant_id="tenant-demo",
        agent_type="fake",
        payload={"prompt": "hello canonical worker"},
        idempotency_key="idem-canonical-worker-demo",
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-0",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    stored_result = calls["store_result"][0] if calls["store_result"] else {}
    result_payload = stored_result.get("payload", {}) if isinstance(stored_result, dict) else {}

    checks = {
        "canonical_worker_consumer_used": True,
        "worker_consumer_process_message_used": True,
        "run_requested_event_serialized": True,
        "run_requested_event_deserialized": bool(calls["should_execute"]),
        "idempotency_guard_should_execute_checked": bool(calls["should_execute"]),
        "idempotency_guard_claimed": bool(calls["try_claim"]),
        "fake_executor_used": True,
        "worker_executor_invoked": bool(executor.invocations),
        "state_store_result_written": bool(calls["store_result"]),
        "state_store_transition_state_called": bool(calls["transition_state"]),
        "state_store_mark_completed_called": bool(calls["mark_completed"]),
        "result_event_written": bool(redis.xadd_calls),
        "message_acknowledged": bool(redis.xack_calls),
        "result_readable": result_payload.get("result") == "success",
        "worker_claim_execute_complete_fallback_used": False,
        "artifact_backed_safe_local_adapter_used": False,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "noncanonical_redis_mutation_attempted": False,
        "operator_action_buttons": False,
    }

    failing_reasons = [
        key for key, value in checks.items()
        if key.endswith("_attempted") and value is True
    ]
    for key in (
        "worker_consumer_process_message_used",
        "idempotency_guard_claimed",
        "worker_executor_invoked",
        "state_store_result_written",
        "state_store_mark_completed_called",
        "message_acknowledged",
        "result_readable",
    ):
        if checks[key] is not True:
            failing_reasons.append(f"{key} is not true")

    artifact = {
        "source": "canonical_worker_task_execution",
        "status": "PASS" if not failing_reasons else "FAIL",
        "tenant_id": event.tenant_id,
        "run_id": event.run_id,
        "agent_type": event.agent_type,
        "worker_id": "worker-canonical-demo",
        "worker_group": "default",
        "shard": 0,
        "executor": "FakeExecutor",
        "result": result_payload,
        "calls": calls,
        "redis_xack_calls": redis.xack_calls,
        "redis_xadd_calls_count": len(redis.xadd_calls),
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
        description="Generate canonical WorkerConsumer task execution binding artifact."
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    return asyncio.run(async_main(args.json))


if __name__ == "__main__":
    raise SystemExit(main_args())
