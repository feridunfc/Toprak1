from __future__ import annotations

import pytest
import time

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagTaskSeed, DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.dag_scheduler_bridge import DagReadyQueue

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.mark.integration
async def test_dag_ready_queue_rebuilds_dispatch_input(redis_client):
    lua = DagLua(redis_client)
    await lua.initialise()
    seed = DagTaskSeed(
        task_id='task-bridge-001',
        run_id='run-bridge-001',
        tenant_id='tenant-bridge',
        agent_type='default',
        priority=7,
        admitted_at=time.time(),
        payload_json='{"k":"v"}',
        dependency_count=0,
    )
    result = await lua.task_admit(seed)
    assert result.ready is True

    queue = DagReadyQueue(redis_client)
    task_id = await queue.peek(seed.tenant_id)
    assert task_id == seed.task_id
    dispatch = await queue.rebuild_dispatch_input(
        task_id,
        worker_group='grp-a',
        shard=0,
        running_zset=DagRedisKey.task_running_zset(seed.tenant_id),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(0),
        region='eu-west',
    )
    assert dispatch is not None
    assert dispatch.task_id == seed.task_id
    assert dispatch.run_id == seed.run_id
    assert dispatch.priority == 7
    assert dispatch.worker_group == 'grp-a'
