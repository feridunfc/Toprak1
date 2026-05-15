
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

from unittest.mock import AsyncMock

from hfa_control.effect_ledger import EFFECT_COMMITTED, EFFECT_REQUESTED, EFFECT_SUPPRESSED, EffectLedger
from hfa_worker.runtime.worker_runtime import WorkerRuntime


class _EventStore:
    def __init__(self, *, fail_on: str | None = None):
        self.fail_on = fail_on
        self.events = []

    async def append_event(self, *, run_id, event_type, worker_id=None, details=None):
        if event_type == self.fail_on:
            return False
        self.events.append((run_id, event_type, worker_id, details or {}))
        return True


class _Redis:
    def __init__(self):
        self.values = {}

    async def exists(self, key):
        return 1 if key in self.values else 0

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True


@pytest.mark.chaos
async def test_worker_effect_cannot_finalize_without_commit_event_ack():
    store = _EventStore(fail_on=EFFECT_COMMITTED)
    runtime = WorkerRuntime(event_store=store, enabled=True)
    effect = AsyncMock(return_value={"ok": True})

    decision = await runtime.execute_effect(
        run_id="run-no-ack",
        worker_id="worker-1",
        token="tok-no-ack",
        effect=effect,
    )

    effect.assert_awaited_once()
    assert decision.executed is True
    assert decision.event_acknowledged is False
    assert decision.finalized is False
    assert decision.reason == "effect_commit_event_not_acknowledged"
    assert [event[1] for event in store.events] == [EFFECT_REQUESTED]


@pytest.mark.chaos
async def test_quarantined_worker_effect_is_suppressed_before_execution():
    store = _EventStore()
    redis = _Redis()
    redis.values["hfa:quarantine:run-q"] = "1"
    runtime = WorkerRuntime(event_store=store, enabled=True)
    effect = AsyncMock(return_value={"should": "not-run"})

    decision = await runtime.execute_effect(
        run_id="run-q",
        worker_id="worker-1",
        token="tok-q",
        effect=effect,
        redis=redis,
    )

    effect.assert_not_awaited()
    assert decision.executed is False
    assert decision.event_acknowledged is True
    assert decision.finalized is False
    assert decision.reason == "quarantined_run_rejected"
    assert [event[1] for event in store.events] == [EFFECT_SUPPRESSED]


@pytest.mark.chaos
async def test_duplicate_worker_effect_is_visible_and_suppressed():
    store = _EventStore()
    redis = _Redis()
    ledger = EffectLedger(redis)
    runtime = WorkerRuntime(event_store=store, effect_ledger=ledger, enabled=True)

    async def effect():
        return {"ok": True}

    first = await runtime.execute_effect(
        run_id="run-dup",
        worker_id="worker-1",
        token="tok-dup",
        effect=effect,
    )
    second_effect = AsyncMock(return_value={"duplicate": True})
    second = await runtime.execute_effect(
        run_id="run-dup",
        worker_id="worker-2",
        token="tok-dup",
        effect=second_effect,
    )

    assert first.executed is True
    assert first.event_acknowledged is True
    assert first.finalized is False
    second_effect.assert_not_awaited()
    assert second.executed is False
    assert second.reason == "duplicate_effect_suppressed"
    assert EFFECT_SUPPRESSED in [event[1] for event in store.events]
