import pytest
from unittest.mock import patch

import fakeredis.aioredis as faredis

from hfa_worker.fake_executor import FakeExecutor
from hfa_worker.main import WorkerService


@pytest.mark.asyncio
async def test_worker_service_uses_explicit_executor():
    redis = faredis.FakeRedis()
    explicit = FakeExecutor()

    config = {
        "worker_id": "test-1",
        "executor": explicit,
    }

    service = WorkerService(redis, config)
    assert service._consumer._executor is explicit


@pytest.mark.asyncio
async def test_worker_service_uses_factory_when_no_explicit():
    redis = faredis.FakeRedis()
    config = {
        "worker_id": "test-2",
        "executor_mode": "fake",
    }

    with patch("hfa_worker.main.build_executor") as mock_build:
        mock_executor = FakeExecutor()
        mock_build.return_value = mock_executor

        service = WorkerService(redis, config)

        mock_build.assert_called_once_with(config)
        assert service._consumer._executor is mock_executor
