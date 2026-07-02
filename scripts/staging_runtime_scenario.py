
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_control.api.task_evidence import read_task_evidence
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor


DEFAULT_ARTIFACT = "local_out/staging_runtime_scenario.json"
SCENARIO_TASK_ID = "staging-runtime-scenario-task"
SCENARIO_RUN_ID = SCENARIO_TASK_ID
SCENARIO_TENANT_ID = "tenant-staging-runtime-scenario"
SCENARIO_WORKER_ID = "worker-staging-runtime-scenario"
SCENARIO_WORKER_GROUP = "staging-runtime-scenario-group"
SCENARIO_SCHEDULER_EPOCH = "scheduler-epoch-staging-runtime-scenario"
SCENARIO_SHARD = 0


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if value is None:
        return ""
    return str(value)


async def _pending_count(redis_client, stream: str) -> int:
    pending = await redis_client.xpending(stream, CONSUMER_GROUP)

    if isinstance(pending, dict):
        if "pending" in pending:
            return int(pending.get("pending") or 0)
        if "count" in pending:
            return int(pending.get("count") or 0)

    if isinstance(pending, (list, tuple)) and pending:
        return int(pending[0] or 0)

    return 0


class StagingScenarioExecutor(TaskExecutor):
    def __init__(self) -> None:
        self.calls: list[TaskContext] = []

    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        self.calls.append(ctx)
        return TaskExecutionResult(
            ok=True,
            output={
                "scenario": "staging_runtime_scenario",
                "done": True,
                "task_id": ctx.task_id,
                "run_id": ctx.run_id,
                "tenant_id": ctx.tenant_id,
                "agent_type": ctx.agent_type,
            },
        )


class ForbiddenLegacyExecutor:
    async def execute(self, run_event):
        raise AssertionError("legacy WorkerConsumer executor path must not run")


def _base_artifact() -> dict[str, Any]:
    return {
        "scenario": "one_command_staging_runtime_scenario",
        "status": "BLOCKED",
        "reason": "",
        "timestamp_ms": int(time.time() * 1000),
        "environment": {
            "python": sys.version.split()[0],
            "redis_available": False,
            "lua_available": False,
            "bridge_flag_enabled": False,
            "real_llm_called": False,
            "deployment_attempted": False,
            "release_tag_created": False,
        },
        "task": {
            "task_id": SCENARIO_TASK_ID,
            "run_id": SCENARIO_RUN_ID,
            "tenant_id": SCENARIO_TENANT_ID,
            "worker_id": SCENARIO_WORKER_ID,
            "worker_group": SCENARIO_WORKER_GROUP,
            "scheduler_epoch": SCENARIO_SCHEDULER_EPOCH,
            "shard": SCENARIO_SHARD,
        },
        "runtime": {
            "stream_message_added": False,
            "message_entered_pending": False,
            "legacy_path_used": None,
            "task_consumer_called": False,
            "task_completed": False,
            "message_acknowledged_after_completion": False,
            "pending_before": None,
            "pending_after": None,
        },
        "evidence": {},
        "safety": {
            "read_only_evidence_fetch": False,
            "redis_mutation_attempted_by_evidence_reader": False,
            "retry_or_reclaim_attempted": False,
            "runtime_repair_attempted": False,
            "production_ready_claim": False,
        },
    }


def _write_artifact(path: Path, artifact: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


async def run_staging_runtime_scenario(
    redis_client,
    *,
    artifact_path: str | Path = DEFAULT_ARTIFACT,
) -> dict[str, Any]:
    """
    Execute a one-command staging-like runtime scenario.

    This intentionally runs a real Redis/Lua runtime drill and then reads
    evidence through the Sprint 66 read-only evidence surface.

    It must not deploy, tag a release, call a real LLM, trigger retry/reclaim,
    or claim production readiness.
    """
    path = Path(artifact_path)
    artifact = _base_artifact()

    previous_bridge_flag = os.environ.get("HFA_WORKER_TASK_CONSUMER_BRIDGE")
    os.environ["HFA_WORKER_TASK_CONSUMER_BRIDGE"] = "1"

    try:
        await redis_client.ping()
        artifact["environment"]["redis_available"] = True
        artifact["environment"]["bridge_flag_enabled"] = True

        stream = RedisKey.stream_shard(SCENARIO_SHARD)

        await ensure_consumer_group(
            redis_client,
            stream,
            CONSUMER_GROUP,
            start_id="0",
            mkstream=True,
        )

        # Scenario setup: this is the runtime drill itself, not dashboard mutation.
        await redis_client.set(DagRedisKey.task_state(SCENARIO_TASK_ID), "scheduled")

        reservation_mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
        reserved = await reservation_mgr.reserve(
            worker_id=SCENARIO_WORKER_ID,
            task_id=SCENARIO_TASK_ID,
            scheduler_epoch=SCENARIO_SCHEDULER_EPOCH,
            reserved_at_ms=123456,
        )
        if not reserved.ok:
            artifact["reason"] = f"reservation_failed:{reserved.reason}"
            _write_artifact(path, artifact)
            return artifact

        dag = DagLua(redis_client)
        artifact["environment"]["lua_available"] = True

        claim_mgr = TaskClaimManager(dag)
        executor = StagingScenarioExecutor()
        task_consumer = TaskConsumer(
            claim_manager=claim_mgr,
            executor=executor,
            completion_manager=dag,
        )

        worker_consumer = WorkerConsumer(
            redis=redis_client,
            worker_id=SCENARIO_WORKER_ID,
            worker_group=SCENARIO_WORKER_GROUP,
            shards=[SCENARIO_SHARD],
            executor=ForbiddenLegacyExecutor(),
            task_consumer=task_consumer,
        )

        legacy_calls: list[str] = []

        async def legacy_should_execute(*args, **kwargs):
            legacy_calls.append("should_execute")
            return True

        async def legacy_try_claim_and_mark_running(*args, **kwargs):
            legacy_calls.append("try_claim_and_mark_running")
            return True

        async def legacy_mark_completed(*args, **kwargs):
            legacy_calls.append("mark_completed")

        async def legacy_release_claim(*args, **kwargs):
            legacy_calls.append("release_claim")

        async def legacy_store_result(*args, **kwargs):
            legacy_calls.append("store_result")

        worker_consumer._guard.should_execute = legacy_should_execute
        worker_consumer._guard.try_claim_and_mark_running = legacy_try_claim_and_mark_running
        worker_consumer._state.mark_completed = legacy_mark_completed
        worker_consumer._state.release_claim = legacy_release_claim
        worker_consumer._state.store_result = legacy_store_result

        event = RunRequestedEvent(
            run_id=SCENARIO_RUN_ID,
            tenant_id=SCENARIO_TENANT_ID,
            agent_type="staging-runtime-scenario-agent",
            payload={"prompt": "prove one-command staging runtime scenario"},
            scheduler_epoch=SCENARIO_SCHEDULER_EPOCH,
            trace_parent="trace-staging-runtime-scenario",
            trace_state="state-staging-runtime-scenario",
        )

        added_msg_id = await redis_client.xadd(stream, serialize_event(event))
        artifact["runtime"]["stream_message_added"] = bool(added_msg_id)

        messages = await redis_client.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=SCENARIO_WORKER_ID,
            streams={stream: ">"},
            count=1,
            block=100,
        )

        if not messages:
            artifact["reason"] = "stream_message_not_read"
            _write_artifact(path, artifact)
            return artifact

        stream_name, entries = messages[0]
        read_msg_id, data = entries[0]

        if _decode(stream_name) != stream:
            artifact["reason"] = "unexpected_stream"
            _write_artifact(path, artifact)
            return artifact

        pending_before = await _pending_count(redis_client, stream)
        artifact["runtime"]["pending_before"] = pending_before
        artifact["runtime"]["message_entered_pending"] = pending_before > 0

        await worker_consumer._process_message(
            msg_id=_decode(read_msg_id),
            data=data,
            stream=stream,
            shard=SCENARIO_SHARD,
        )

        pending_after = await _pending_count(redis_client, stream)
        artifact["runtime"]["pending_after"] = pending_after
        artifact["runtime"]["legacy_path_used"] = bool(legacy_calls)
        artifact["runtime"]["task_consumer_called"] = bool(executor.calls)
        artifact["runtime"]["message_acknowledged_after_completion"] = pending_after == 0

        evidence = await read_task_evidence(redis_client, SCENARIO_TASK_ID)
        artifact["safety"]["read_only_evidence_fetch"] = True
        artifact["evidence"] = evidence

        artifact["runtime"]["task_completed"] = (
            evidence.get("state") == "done"
            and evidence.get("terminal_state") == "done"
            and evidence.get("output_found") is True
        )

        pass_conditions = [
            artifact["environment"]["redis_available"],
            artifact["environment"]["lua_available"],
            artifact["environment"]["bridge_flag_enabled"],
            artifact["runtime"]["stream_message_added"],
            artifact["runtime"]["message_entered_pending"],
            artifact["runtime"]["legacy_path_used"] is False,
            artifact["runtime"]["task_consumer_called"],
            artifact["runtime"]["task_completed"],
            artifact["runtime"]["message_acknowledged_after_completion"],
            artifact["evidence"].get("worker_instance_id") == SCENARIO_WORKER_ID,
            artifact["evidence"].get("scheduler_epoch") == SCENARIO_SCHEDULER_EPOCH,
            artifact["evidence"].get("claim_epoch") == "1",
            artifact["safety"]["production_ready_claim"] is False,
            artifact["environment"]["real_llm_called"] is False,
            artifact["environment"]["deployment_attempted"] is False,
            artifact["environment"]["release_tag_created"] is False,
        ]

        if all(pass_conditions):
            artifact["status"] = "PASS"
            artifact["reason"] = "scenario_completed"
        else:
            artifact["status"] = "BLOCKED"
            artifact["reason"] = "scenario_incomplete"

        _write_artifact(path, artifact)
        return artifact

    except Exception as exc:
        artifact["status"] = "BLOCKED"
        artifact["reason"] = f"{type(exc).__name__}: {exc}"
        _write_artifact(path, artifact)
        return artifact

    finally:
        if previous_bridge_flag is None:
            os.environ.pop("HFA_WORKER_TASK_CONSUMER_BRIDGE", None)
        else:
            os.environ["HFA_WORKER_TASK_CONSUMER_BRIDGE"] = previous_bridge_flag


async def _main_async(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one-command staging runtime scenario")
    parser.add_argument("--out", default=DEFAULT_ARTIFACT, help="Artifact JSON output path")
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        help="Redis URL for staging/local scenario",
    )
    args = parser.parse_args(argv)

    try:
        import redis.asyncio as redis_asyncio
    except Exception as exc:
        artifact = _base_artifact()
        artifact["reason"] = f"redis_client_import_failed:{type(exc).__name__}: {exc}"
        _write_artifact(Path(args.out), artifact)
        return 1

    redis_client = redis_asyncio.from_url(args.redis_url, decode_responses=False)
    try:
        artifact = await run_staging_runtime_scenario(redis_client, artifact_path=args.out)
        print(json.dumps(artifact, indent=2, sort_keys=True))
        return 0 if artifact.get("status") == "PASS" else 1
    finally:
        close = getattr(redis_client, "aclose", None)
        if close is not None:
            await close()
        else:
            await redis_client.close()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main_async(argv))


if __name__ == "__main__":
    raise SystemExit(main())
