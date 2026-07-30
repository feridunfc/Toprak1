from __future__ import annotations

import inspect
from pathlib import Path

from hfa_control.api.models import RunStateResponse
from hfa_control.recovery import RecoveryService
from hfa_control.service import ControlPlaneService
from hfa_control.task_recovery import TaskRecoveryManager


ROOT = Path(__file__).resolve().parents[2]


def _source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_task_requeue_binds_operation_and_fixed_conflict_identity_order() -> None:
    source = _source("hfa-core/src/hfa/lua/task_requeue.lua")

    assert "local OPERATION = 'TASK_REQUEUE'" in source
    material = source.index("local material = length_prefix(OPERATION)")
    run_id = source.index(".. length_prefix(run_id)", material)
    task_id = source.index(".. length_prefix(task_id)", run_id)
    status = source.index(".. length_prefix(status)", task_id)
    detail = source.index(".. length_prefix(detail_code)", status)
    observed = source.index(".. length_prefix(observed_run_state or '')", detail)
    assert material < run_id < task_id < status < detail < observed

    assert "'operation', OPERATION" in source
    assert "existing.operation ~= OPERATION" in source


def test_task_requeue_validates_authoritative_identity_before_run_truth() -> None:
    source = _source("hfa-core/src/hfa/lua/task_requeue.lua")

    meta_type = source.index("local meta_kind = redis_type(task_meta_key)")
    membership = source.index("SISMEMBER', run_tasks_set, task_id")
    run_type = source.index("local run_kind = redis_type(run_state_key)")
    first_mutation = source.index("redis.call('ZREM', task_running_zset")
    assert meta_type < membership < run_type < first_mutation


def test_task_recovery_passes_run_truth_membership_and_conflict_keys() -> None:
    source = inspect.getsource(TaskRecoveryManager.requeue_stale_task)

    assert 'run_id: str = ""' in source
    expected = (
        "RedisKey.run_state(resolved_run_id)",
        "DagRedisKey.run_tasks(resolved_run_id)",
        "RedisKey.runtime_truth_conflict_index()",
        "RedisKey.runtime_truth_conflict_stream()",
    )
    positions = [source.index(token) for token in expected]
    assert positions == sorted(positions)


def test_run_recovery_commit_revalidates_dynamic_task_contract_before_mutation() -> None:
    source = _source("hfa-core/src/hfa/lua/run_recovery_commit.lua")

    assert "local OPERATION = 'RUN_RECOVERY'" in source
    cardinality = source.index("recovery_contract_cardinality_mismatch")
    task_index = source.index("local task_set_kind = redis_type(run_tasks_set)")
    task_loop = source.index("for index = 1, task_count do")
    count_check = source.index("stored_count ~= expected_reschedule_count")
    first_state_write = source.index("redis.call('SET', run_state_key, 'rescheduled'")
    assert cardinality < task_index < task_loop < count_check < first_state_write


def test_run_recovery_state_projection_and_events_share_one_lua_commit() -> None:
    lua = _source("hfa-core/src/hfa/lua/run_recovery_commit.lua")
    python_source = inspect.getsource(RecoveryService._commit_recovery)
    reschedule_source = inspect.getsource(RecoveryService._reschedule)

    state_write = lua.index("redis.call('SET', run_state_key, 'rescheduled'")
    projection_write = lua.index("redis.call('ZADD', running_zset", state_write)
    rescheduled_event = lua.index("'event_type', 'RunRescheduled'", projection_write)
    admitted_event = lua.index("'event_type', 'RunAdmitted'", rescheduled_event)
    final_return = lua.index("return {'RUN_RESCHEDULED'", admitted_event)
    assert state_write < projection_write < rescheduled_event < admitted_event < final_return
    assert "self._config.control_stream" in python_source
    assert "RedisTTL.STREAM_MAXLEN" in python_source
    assert ".xadd(" not in reschedule_source


def test_mock_run_recovery_fallback_is_mutation_free() -> None:
    source = inspect.getsource(RecoveryService._commit_recovery_fallback)

    assert "truth_conflict_evidence_store_unavailable" in source
    for forbidden in (".set(", ".hset(", ".zadd(", ".zrem(", ".xadd(", ".delete("):
        assert forbidden not in source


def test_stale_discovery_is_read_only_and_does_not_choose_truth_winner() -> None:
    source = inspect.getsource(RecoveryService._find_stale_runs)

    assert "zrangebyscore" in source
    assert ".get(" not in source
    assert "zrem" not in source
    assert "hset" not in source
    assert "xadd" not in source


def test_run_state_read_model_is_read_only_and_returns_scoped_conflicts() -> None:
    source = inspect.getsource(ControlPlaneService.get_run_state)

    assert "task_truth" in source
    assert "truth_conflicts" in source
    assert '"state": run_state' in source
    for forbidden in (".set(", ".hset(", ".zadd(", ".zrem(", ".xadd(", ".delete("):
        assert forbidden not in source


def test_run_state_response_additive_defaults_preserve_legacy_callers() -> None:
    response = RunStateResponse(
        run_id="run-1",
        tenant_id="tenant-1",
        state="running",
        worker_group="workers",
        shard=0,
        reschedule_count=0,
        admitted_at=1.0,
    )
    data = response.model_dump()

    assert data["state"] == "running"
    assert data["task_count"] == 0
    assert data["task_truth"] == []
    assert data["truth_status"] == "consistent"
    assert data["truth_conflict"] is False
    assert data["truth_conflicts"] == []
