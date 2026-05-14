
import pytest
from unittest.mock import AsyncMock

from hfa_control.decision_metrics import DecisionMetrics

pytestmark = pytest.mark.asyncio


async def test_metric_increment_returns_new_value():
    redis = AsyncMock()
    redis.incrby.return_value = 3

    metrics = DecisionMetrics(redis)
    result = await metrics.incr("dispatch_success_total", 3)

    assert result.metric_name == "dispatch_success_total"
    assert result.new_value == 3


async def test_metric_get_defaults_zero():
    redis = AsyncMock()
    redis.get.return_value = None

    metrics = DecisionMetrics(redis)
    value = await metrics.get("dispatch_success_total")

    assert value == 0
