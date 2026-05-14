
import pytest

from hfa.runtime.idempotency_store import IdempotencyStore
from hfa_control.idempotent_completion import IdempotentCompletionGuard
from hfa_control.idempotent_dispatch import IdempotentDispatchGuard

pytestmark = pytest.mark.asyncio


@pytest.mark.chaos
async def test_duplicate_dispatch_storm_first_write_wins(redis_client):
    store = IdempotencyStore(redis_client)
    guard = IdempotentDispatchGuard(store)

    first = await guard.guard(task_id="storm-task", run_id="run-1", worker_id="worker-a")
    second = await guard.guard(task_id="storm-task", run_id="run-1", worker_id="worker-b")
    third = await guard.guard(task_id="storm-task", run_id="run-1", worker_id="worker-c")

    assert first.ok is True
    assert second.ok is False
    assert third.ok is False
    assert second.existing_value == "run-1:worker-a"
    assert third.existing_value == "run-1:worker-a"


@pytest.mark.chaos
async def test_duplicate_completion_storm_first_write_wins(redis_client):
    store = IdempotencyStore(redis_client)
    guard = IdempotentCompletionGuard(store)

    first = await guard.guard(task_id="storm-complete", run_id="run-2", worker_id="worker-a")
    second = await guard.guard(task_id="storm-complete", run_id="run-2", worker_id="worker-b")

    assert first.ok is True
    assert second.ok is False
    assert second.existing_value == "run-2:worker-a"
