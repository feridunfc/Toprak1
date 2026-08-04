from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import redis.asyncio as redis

from hfa.config.keys import RedisKey
from hfa.dag.schema import (
    DagRedisKey,
    DagTaskDispatchInput,
    DagTaskSeed,
)
from hfa_control.dag_lua import DagLua
from hfa_control.task_dispatch_authority import (
    TaskDispatchAuthorityConflictError,
    task_dispatch_identity,
)


def _seed(acceptance_id: str) -> DagTaskSeed:
    safe = "".join(
        character
        for character in acceptance_id
        if character.isalnum() or character in "-_"
    ) or "s84-1"
    return DagTaskSeed(
        task_id=f"{safe}-task",
        run_id=f"{safe}-run",
        tenant_id=f"{safe}-tenant",
        agent_type="default",
        priority=5,
        admitted_at=1000,
        dependency_count=0,
        payload_json='{"acceptance":true}',
        region="eu",
        policy="LEAST_LOADED",
    )


def _dispatch(
    item: DagTaskSeed,
    *,
    worker_id: str = "worker-1",
    scheduled_at: int = 2000,
) -> DagTaskDispatchInput:
    return DagTaskDispatchInput(
        task_id=item.task_id,
        run_id=item.run_id,
        tenant_id=item.tenant_id,
        worker_id=worker_id,
        worker_group="group-1",
        agent_type=item.agent_type,
        shard=2,
        priority=item.priority,
        admitted_at=int(item.admitted_at),
        scheduled_at=scheduled_at,
        scheduled_zset=(
            DagRedisKey.task_scheduled_zset(
                item.tenant_id
            )
        ),
        running_zset=(
            DagRedisKey.task_running_zset(
                item.tenant_id
            )
        ),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(2),
        region=item.region,
        policy=item.policy,
        payload_json=item.payload_json,
        scheduler_epoch="epoch-7",
        attempt=1,
    )


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    client = redis.from_url(
        args.redis_url,
        decode_responses=True,
    )
    try:
        if args.reset_test_db:
            await client.flushdb()

        item = _seed(args.acceptance_id)
        await client.set(
            RedisKey.run_state(item.run_id),
            "admitted",
        )
        dag = DagLua(
            client,
            canonical_task_admit_binding=True,
            canonical_task_dispatch_binding=True,
        )
        admitted = await dag.task_admit(item)
        value = _dispatch(item)
        first = await dag.task_dispatch_commit(value)
        first_control_len = await client.xlen(
            value.control_stream
        )
        first_shard_len = await client.xlen(
            value.shard_stream
        )

        replay = await dag.task_dispatch_commit(
            _dispatch(
                item,
                scheduled_at=9999,
            )
        )
        replay_control_len = await client.xlen(
            value.control_stream
        )
        replay_shard_len = await client.xlen(
            value.shard_stream
        )

        conflict_status = ""
        try:
            await dag.task_dispatch_commit(
                _dispatch(
                    item,
                    worker_id="worker-2",
                    scheduled_at=3000,
                )
            )
        except TaskDispatchAuthorityConflictError as exc:
            conflict_status = exc.status

        meta = await client.hgetall(
            DagRedisKey.task_meta(item.task_id)
        )
        binding = dag._task_dispatch_authority_binding
        snapshot = (
            await binding.store.get_aggregate_snapshot(
                task_dispatch_identity(value)
            )
            if binding is not None
            else None
        )

        report = {
            "schema_version": 1,
            "sprint": "84.1",
            "status": "PASS_WITH_LIMITATIONS",
            "acceptance_id": args.acceptance_id,
            "canonical_task_admit_enabled": True,
            "canonical_task_dispatch_enabled": True,
            "task_admitted": bool(admitted.admitted),
            "first_dispatch_status": first.status,
            "first_dispatch_committed": bool(
                first.committed
            ),
            "canonical_revision": (
                None
                if snapshot is None
                else snapshot.revision
            ),
            "canonical_state": (
                None
                if snapshot is None
                else snapshot.state
            ),
            "legacy_task_state": await client.get(
                DagRedisKey.task_state(item.task_id)
            ),
            "projection_transition_bound": bool(
                meta.get("canonical_transition_id")
            ),
            "projection_record_hash_bound": bool(
                meta.get("canonical_record_hash")
            ),
            "projection_command_hash_bound": bool(
                meta.get("canonical_command_hash")
            ),
            "dispatch_attempt": int(
                meta.get("dispatch_attempt", "0")
            ),
            "dispatch_worker_id": meta.get(
                "dispatch_worker_id",
                "",
            ),
            "replay_status": replay.status,
            "replay_committed": bool(
                replay.committed
            ),
            "replay_stream_reemission": (
                replay_control_len
                != first_control_len
                or replay_shard_len
                != first_shard_len
            ),
            "same_attempt_changed_worker_status": (
                conflict_status
            ),
            "automatic_reassignment": False,
            "automatic_repair": False,
            "canonical_requeue_binding": False,
            "production_ready": False,
            "production_cutover_authorized": False,
        }

        assert report["task_admitted"] is True
        assert (
            report["first_dispatch_status"]
            == "committed"
        )
        assert report["canonical_revision"] == 2
        assert report["canonical_state"] == "scheduled"
        assert report["legacy_task_state"] == "scheduled"
        assert report["projection_transition_bound"] is True
        assert report["projection_record_hash_bound"] is True
        assert report["projection_command_hash_bound"] is True
        assert report["dispatch_attempt"] == 1
        assert report["dispatch_worker_id"] == "worker-1"
        assert report["replay_status"] == "already_projected"
        assert report["replay_stream_reemission"] is False
        assert (
            report["same_attempt_changed_worker_status"]
            == "IDEMPOTENCY_CONFLICT"
        )
        return report
    finally:
        await client.aclose()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-url",
        default="redis://127.0.0.1:6389/0",
    )
    parser.add_argument(
        "--acceptance-id",
        default="s84-1-local",
    )
    parser.add_argument(
        "--reset-test-db",
        action="store_true",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--json",
        action="store_true",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    report = asyncio.run(_run(args))
    encoded = json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    args.out.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    args.out.write_text(
        encoded,
        encoding="utf-8",
        newline="\n",
    )
    if args.json:
        print(encoded, end="")
    else:
        print(
            "SPRINT84_1_ACCEPTANCE",
            report["status"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
