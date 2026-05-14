
import asyncio
import pytest
from unittest.mock import AsyncMock

from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_worker.task_heartbeat import HeartbeatLoop


pytestmark = pytest.mark.asyncio


async def test_heartbeat_loop_records_heartbeat():
    mgr = AsyncMock(spec=TaskHeartbeatManager)

    loop = HeartbeatLoop(
        heartbeat_manager=mgr,
        task_id="task-1",
        tenant_id="tenant-a",
        worker_instance_id="worker-1",
        interval_ms=20,
    )
    await loop.start()
    await asyncio.sleep(0.06)
    await loop.stop()

    assert mgr.record_heartbeat.await_count >= 1


async def test_heartbeat_loop_stop_is_clean():
    mgr = AsyncMock(spec=TaskHeartbeatManager)

    loop = HeartbeatLoop(
        heartbeat_manager=mgr,
        task_id="task-1",
        tenant_id="tenant-a",
        worker_instance_id="worker-1",
        interval_ms=20,
    )
    await loop.start()
    await asyncio.sleep(0.03)
    await loop.stop()

    assert loop._task is None
