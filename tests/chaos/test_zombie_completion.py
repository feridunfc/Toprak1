
import pytest

from hfa.runtime.idempotency_store import IdempotencyStore
from hfa_control.idempotent_completion import IdempotentCompletionGuard

pytestmark = pytest.mark.asyncio


@pytest.mark.chaos
async def test_zombie_worker_cannot_overwrite_completion(redis_client):
    store = IdempotencyStore(redis_client)
    guard = IdempotentCompletionGuard(store)

    hero = await guard.guard(task_id="task-zombie", run_id="run-hero", worker_id="hero-worker")
    zombie = await guard.guard(task_id="task-zombie", run_id="run-zombie", worker_id="zombie-worker")

    assert hero.ok is True
    assert zombie.ok is False
    assert zombie.status == "completion_duplicate"
    assert zombie.existing_value == "run-hero:hero-worker"
