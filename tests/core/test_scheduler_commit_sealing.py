from __future__ import annotations

import pytest

from hfa_control.event_store import EventStore
from hfa_control.scheduler_loop import SchedulerLoop
from hfa_control.scheduler_candidate.shadow_dispatcher import ShadowDispatcher

pytestmark = pytest.mark.asyncio


class _Store:
    def __init__(self, ok: bool = True):
        self.ok = ok
        self.calls = []

    async def append_event(self, *, run_id, event_type, worker_id=None, details=None):
        self.calls.append((run_id, event_type, worker_id, details))
        return self.ok


class _Controller:
    def __init__(self, result):
        self.result = result

    async def dispatch_once(self, **kwargs):
        return dict(self.result)


async def test_scheduler_seal_appends_scheduled_event_before_authoritative_result(monkeypatch):
    monkeypatch.setenv("IRON_V3_SCHEDULER_SEAL", "1")
    store = _Store(ok=True)
    loop = SchedulerLoop(
        dispatch_controller=_Controller({"run_id": "run-1", "worker_id": "worker-1", "tenant_id": "tenant-a"}),
        event_store=store,
    )
    loop._epoch = "e1"

    ok = await loop._dispatch_once(snapshot=object())

    assert ok is True
    assert store.calls == [
        (
            "run-1",
            EventStore.EVENT_TASK_SCHEDULED,
            "worker-1",
            {
                "scheduler_epoch": "e1",
                "tenant_id": "tenant-a",
                "authority": "SchedulerLoop.dispatch_commit",
                "event_gate": "IRON_V3_SCHEDULER_SEAL",
            },
        )
    ]


async def test_scheduler_seal_blocks_authoritative_result_when_append_fails(monkeypatch):
    monkeypatch.setenv("IRON_V3_SCHEDULER_SEAL", "1")
    store = _Store(ok=False)
    loop = SchedulerLoop(
        dispatch_controller=_Controller({"run_id": "run-2", "worker_id": "worker-2"}),
        event_store=store,
    )

    ok = await loop._dispatch_once(snapshot=object())

    assert ok is False
    assert len(store.calls) == 1
    assert store.calls[0][1] == EventStore.EVENT_TASK_SCHEDULED


async def test_scheduler_legacy_background_emission_when_flag_disabled(monkeypatch):
    monkeypatch.delenv("IRON_V3_SCHEDULER_SEAL", raising=False)
    store = _Store(ok=True)
    loop = SchedulerLoop(
        dispatch_controller=_Controller({"run_id": "run-3", "worker_id": "worker-3"}),
        event_store=store,
    )

    ok = await loop._dispatch_once(snapshot=object())

    assert ok is True


async def test_shadow_dispatcher_is_non_authoritative():
    dispatcher = ShadowDispatcher()
    record = await dispatcher.dispatch(run_id="run-shadow", worker_id="worker-shadow")

    assert dispatcher.authoritative is False
    assert record.authoritative is False
    assert record.reason == "shadow_dispatch_not_authoritative"
