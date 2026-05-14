
import pytest
from unittest.mock import AsyncMock

from hfa_control.reconciliation_manager import ReconciliationManager

pytestmark = pytest.mark.asyncio


async def test_reconcile_noop_when_expected_matches_actual():
    redis = AsyncMock()
    redis.get.return_value = "2"
    redis.zcard.return_value = 2

    mgr = ReconciliationManager(redis)
    report = await mgr.reconcile_tenant("tenant-a")

    assert report.corrected is False
    redis.set.assert_not_awaited()


async def test_reconcile_corrects_when_expected_differs_from_actual():
    redis = AsyncMock()
    redis.get.return_value = "5"
    redis.zcard.return_value = 2

    mgr = ReconciliationManager(redis)
    report = await mgr.reconcile_tenant("tenant-a")

    assert report.corrected is True
    assert report.expected_inflight == 5
    assert report.actual_inflight == 2
    redis.set.assert_awaited_once()
