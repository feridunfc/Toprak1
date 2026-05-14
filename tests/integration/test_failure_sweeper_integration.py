
import pytest

from hfa_control.failure_sweeper import FailureSweeper
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


async def test_pending_child_becomes_blocked(redis_client):
    parent = "p1"
    child = "c1"

    await redis_client.sadd(DagRedisKey.task_children(parent), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")

    sweeper = FailureSweeper(redis_client)
    count = await sweeper.sweep_failed_task(parent)

    assert count == 1
    state = await redis_client.get(DagRedisKey.task_state(child))
    assert state == "blocked_by_failure"


async def test_ready_child_becomes_blocked(redis_client):
    parent = "p2"
    child = "c2"

    await redis_client.sadd(DagRedisKey.task_children(parent), child)
    await redis_client.set(DagRedisKey.task_state(child), "ready")

    sweeper = FailureSweeper(redis_client)
    count = await sweeper.sweep_failed_task(parent)

    assert count == 1
    state = await redis_client.get(DagRedisKey.task_state(child))
    assert state == "blocked_by_failure"


async def test_terminal_child_untouched(redis_client):
    parent = "p3"
    child_done = "c3d"
    child_failed = "c3f"

    await redis_client.sadd(DagRedisKey.task_children(parent), child_done, child_failed)
    await redis_client.set(DagRedisKey.task_state(child_done), "done")
    await redis_client.set(DagRedisKey.task_state(child_failed), "failed")

    sweeper = FailureSweeper(redis_client)
    count = await sweeper.sweep_failed_task(parent)

    assert count == 0
    assert await redis_client.get(DagRedisKey.task_state(child_done)) == "done"
    assert await redis_client.get(DagRedisKey.task_state(child_failed)) == "failed"


async def test_idempotent_sweep(redis_client):
    parent = "p4"
    child = "c4"

    await redis_client.sadd(DagRedisKey.task_children(parent), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")

    sweeper = FailureSweeper(redis_client)

    first = await sweeper.sweep_failed_task(parent)
    second = await sweeper.sweep_failed_task(parent)

    assert first == 1
    assert second == 0

    state = await redis_client.get(DagRedisKey.task_state(child))
    assert state == "blocked_by_failure"
