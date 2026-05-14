
import pytest
from unittest.mock import AsyncMock

from hfa_control.task_recovery import TaskHeartbeatManager, TaskRecoveryManager
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.reasons import TASK_HEARTBEAT_RECORDED, TASK_HEARTBEAT_OWNER_MISMATCH


pytestmark = pytest.mark.asyncio


async def test_record_heartbeat_accepts_running_owner():
    redis = AsyncMock()
    redis.get.return_value = "running"
    redis.hget.return_value = "worker-1"

    mgr = TaskHeartbeatManager(redis)
    result = await mgr.record_heartbeat(task_id="t1", tenant_id="tenant-a", worker_id="worker-1", now_ms=123)

    assert result.ok is True
    assert result.status == TASK_HEARTBEAT_RECORDED


async def test_record_heartbeat_rejects_owner_mismatch():
    redis = AsyncMock()
    redis.get.return_value = "running"
    redis.hget.return_value = "worker-2"

    mgr = TaskHeartbeatManager(redis)
    result = await mgr.record_heartbeat(task_id="t1", tenant_id="tenant-a", worker_id="worker-1", now_ms=123)

    assert result.ok is False
    assert result.status == TASK_HEARTBEAT_OWNER_MISMATCH


async def test_find_stale_tasks_detects_old_heartbeats():
    redis = AsyncMock()
    redis.zrange.return_value = ["t1", "t2"]
    redis.hget.side_effect = ["1000", "45000"]

    mgr = TaskRecoveryManager(redis, HeartbeatPolicy(stale_after_ms=10_000))
    stale = await mgr.find_stale_tasks(tenant_id="tenant-a", now_ms=20_000)

    assert stale == ["t1"]
