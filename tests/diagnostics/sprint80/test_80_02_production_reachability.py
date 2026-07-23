from __future__ import annotations

import inspect

import pytest

from hfa_control.scheduler import build_production_scheduler
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.main import WorkerService


class _Executor:
    async def execute(self, event):
        return type(
            "ExecutionResult",
            (),
            {
                "status": "done",
                "payload": {},
                "error": "",
                "cost_cents": 0,
                "tokens_used": 0,
            },
        )()


class _ShardManager:
    pass


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_production_worker_composes_taskconsumer(sprint80_redis):
    service = WorkerService(
        sprint80_redis,
        {
            "production": True,
            "worker_id": "s80-worker",
            "worker_group": "s80-group",
            "region": "test",
            "version": "s80",
            "capabilities": ["base"],
            "shards": [0],
            "capacity": 1,
            "executor": _Executor(),
            "shard_manager": _ShardManager(),
            "task_heartbeat_interval_ms": 1000,
            "task_stale_after_ms": 5000,
        },
    )

    assert service._task_consumer is not None
    assert service._consumer._task_consumer is service._task_consumer


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_production_worker_composes_daglua_completion(sprint80_redis):
    service = WorkerService(
        sprint80_redis,
        {
            "production": True,
            "worker_id": "s80-worker",
            "worker_group": "s80-group",
            "executor": _Executor(),
            "shard_manager": _ShardManager(),
            "task_heartbeat_interval_ms": 1000,
            "task_stale_after_ms": 5000,
        },
    )

    assert service._dag_lua is not None
    assert service._task_consumer._completion_manager is service._dag_lua


@pytest.mark.sprint80_reality
def test_taskrequested_event_type_selects_modern_taskconsumer_path():
    source = inspect.getsource(WorkerConsumer._process_message)
    assert 'event_type == "TaskRequested"' in source
    assert "_process_message_via_task_consumer" in source


@pytest.mark.sprint80_reality
def test_runrequested_legacy_worker_path_remains_reachable():
    source = inspect.getsource(WorkerConsumer._process_message)
    assert "try_claim_and_mark_running" in source
    assert "_process_message_via_task_consumer" in source


@pytest.mark.sprint80_reality
def test_scheduler_event_store_is_optional_in_production_composition():
    signature = inspect.signature(build_production_scheduler)
    assert signature.parameters["event_store"].default is None


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_semantic_components_are_not_in_production_worker_composition(sprint80_redis):
    service = WorkerService(
        sprint80_redis,
        {
            "production": True,
            "worker_id": "s80-worker",
            "worker_group": "s80-group",
            "executor": _Executor(),
            "shard_manager": _ShardManager(),
            "task_heartbeat_interval_ms": 1000,
            "task_stale_after_ms": 5000,
        },
    )

    modules = {
        type(component).__module__
        for component in (
            service,
            service._consumer,
            service._task_consumer,
            service._task_claim_manager,
            service._task_heartbeat_manager,
            service._dag_lua,
        )
        if component is not None
    }
    assert all(not module.startswith("hfa_semantic") for module in modules)
