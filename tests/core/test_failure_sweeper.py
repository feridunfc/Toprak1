import pytest

from hfa_control.failure_sweeper import FailureSweeper
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


async def test_failure_sweeper_blocks_children(mock_redis):
    parent = "task-parent"
    child = "task-child"

    # AsyncMock stateful değil; beklenen dönüşleri açıkça tanımla
    mock_redis.smembers.return_value = {child}
    mock_redis.get.return_value = "pending"
    mock_redis.set.return_value = True

    sweeper = FailureSweeper(mock_redis)
    count = await sweeper.sweep_failed_task(parent)

    assert count == 1
    mock_redis.smembers.assert_awaited_once_with(DagRedisKey.task_children(parent))
    mock_redis.get.assert_awaited_once_with(DagRedisKey.task_state(child))
    mock_redis.set.assert_awaited_once_with(DagRedisKey.task_state(child), "blocked_by_failure")