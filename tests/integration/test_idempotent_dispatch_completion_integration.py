
import pytest

from hfa.runtime.idempotency_store import IdempotencyStore
from hfa_control.idempotent_completion import IdempotentCompletionGuard
from hfa_control.idempotent_dispatch import IdempotentDispatchGuard

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_same_dispatch_twice_only_first_is_accepted(redis_client):
    store = IdempotencyStore(redis_client)
    guard = IdempotentDispatchGuard(store)

    first = await guard.guard(task_id="task-1", run_id="run-1", worker_id="worker-1")
    second = await guard.guard(task_id="task-1", run_id="run-1", worker_id="worker-2")

    assert first.ok is True
    assert first.status == "dispatch_token_acquired"
    assert second.ok is False
    assert second.status == "dispatch_duplicate"
    assert second.existing_value == "run-1:worker-1"


@pytest.mark.integration
async def test_same_completion_twice_only_first_is_accepted(redis_client):
    store = IdempotencyStore(redis_client)
    guard = IdempotentCompletionGuard(store)

    first = await guard.guard(task_id="task-2", run_id="run-2", worker_id="worker-1")
    second = await guard.guard(task_id="task-2", run_id="run-2", worker_id="worker-2")

    assert first.ok is True
    assert first.status == "completion_token_acquired"
    assert second.ok is False
    assert second.status == "completion_duplicate"
    assert second.existing_value == "run-2:worker-1"


@pytest.mark.integration
async def test_dispatch_and_completion_use_separate_tokens(redis_client):
    store = IdempotencyStore(redis_client)
    dispatch_guard = IdempotentDispatchGuard(store)
    completion_guard = IdempotentCompletionGuard(store)

    dispatch = await dispatch_guard.guard(task_id="task-3", run_id="run-3", worker_id="worker-1")
    completion = await completion_guard.guard(task_id="task-3", run_id="run-3", worker_id="worker-1")

    assert dispatch.ok is True
    assert completion.ok is True
    assert dispatch.token_key != completion.token_key
