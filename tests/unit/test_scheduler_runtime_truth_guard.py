from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from hfa.config.keys import RedisKey
from hfa_control.dag_lua import DagLua


REPO_ROOT = Path(__file__).resolve().parents[2]
LUA_PATH = REPO_ROOT / "hfa-core/src/hfa/lua/task_dispatch_commit.lua"


@dataclass
class _Dispatch:
    task_id: str = "task-1"
    run_id: str = "run-1"
    tenant_id: str = "tenant-1"
    agent_type: str = "agent"
    worker_group: str = "workers"
    shard: int = 2
    priority: int = 5
    admitted_at: int = 100
    scheduled_at: int = 200
    scheduler_epoch: str = "epoch-1"
    payload_json: str = "{}"
    trace_parent: str = ""
    trace_state: str = ""
    policy: str = "LEAST_LOADED"
    region: str = "eu"
    scheduled_zset: str = ""
    running_zset: str = ""
    control_stream: str = ""
    shard_stream: str = ""


class _Loader:
    def __init__(self) -> None:
        self.call = None

    async def run(self, *, num_keys, keys, args):
        self.call = (num_keys, keys, args)
        return ["run_truth_missing", "run_state_missing"]


class _Redis:
    pass


def test_runtime_truth_key_builders_are_centralized_and_not_authority_namespace():
    index = RedisKey.runtime_truth_conflict_index()
    stream = RedisKey.runtime_truth_conflict_stream()
    assert index == "hfa:runtime-truth:v1:conflicts:index"
    assert stream == "hfa:runtime-truth:v1:conflicts:stream"
    assert "hfa:authority:v1" not in index
    assert "hfa:authority:v1" not in stream


def test_lua_declares_run_and_global_conflict_keys():
    source = LUA_PATH.read_text(encoding="utf-8")
    assert "local run_state_key         = KEYS[8]" in source
    assert "local truth_conflict_index  = KEYS[9]" in source
    assert "local truth_conflict_stream = KEYS[10]" in source


def test_lua_known_runtime_truth_state_sets_are_exact():
    source = LUA_PATH.read_text(encoding="utf-8")
    for state in ("admitted", "queued", "scheduled", "running", "rescheduled"):
        assert f"{state}=true" in source
    for state in ("done", "failed", "rejected", "dead_lettered"):
        assert f"{state}=true" in source
    assert "blocked_by_failure=true" not in source
    assert "skipped=true" not in source


def test_lua_exposes_required_structured_statuses():
    source = LUA_PATH.read_text(encoding="utf-8")
    for status in (
        "run_truth_missing",
        "run_truth_terminal_conflict",
        "run_truth_corruption_conflict",
        "truth_conflict_evidence_store_unavailable",
    ):
        assert status in source


def test_lua_guard_precedes_first_lifecycle_mutation():
    source = LUA_PATH.read_text(encoding="utf-8")
    guard = source.index("local run_kind = redis_type(run_state_key)")
    first_mutation = source.index("redis.call('ZREM', tenant_ready_queue")
    assert guard < first_mutation
    assert source.index("emit_truth_conflict", guard) < first_mutation


def test_conflict_identity_is_length_prefixed_deterministic_and_first_payload_wins():
    source = LUA_PATH.read_text(encoding="utf-8")
    assert "local function length_prefix" in source
    assert "local conflict_id = redis.sha1hex(material)" in source
    assert "HSETNX" in source
    assert "if inserted == 1 then" in source
    assert source.count("redis.call('XADD', truth_conflict_stream") == 1


@pytest.mark.asyncio
async def test_dag_lua_passes_run_truth_and_global_conflict_keys_as_8_9_10():
    dag = DagLua(_Redis())
    loader = _Loader()
    dag._admit_loader = object()
    dag._dispatch_loader = loader

    result = await dag.task_dispatch_commit(_Dispatch())

    assert result.status == "run_truth_missing"
    assert result.reason == "run_state_missing"
    assert loader.call is not None
    num_keys, keys, _ = loader.call
    assert num_keys == 10
    assert keys[7] == RedisKey.run_state("run-1")
    assert keys[8] == RedisKey.runtime_truth_conflict_index()
    assert keys[9] == RedisKey.runtime_truth_conflict_stream()
