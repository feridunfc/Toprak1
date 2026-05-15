from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa.events.apply_event import replay_completion_slice
from hfa.runtime.state_store import StateStore
from hfa_core.events.event_types import (
    TASK_COMPLETION_REQUESTED,
    TASK_COMPLETED,
    TASK_FAILED,
)


class FakeControlStore:
    def __init__(self, *, owner: str | None = None, state: str | None = None) -> None:
        self.owner = owner
        self.state = state
        self.state_writes: list[tuple[str, str]] = []
        self.owner_writes: list[tuple[str, str]] = []

    async def reserve_worker(self, **kwargs):
        return None

    async def update_vruntime(self, **kwargs):
        return None

    async def get_owner(self, *, task_id: str):
        return self.owner

    async def set_owner(self, *, task_id: str, worker_id: str) -> None:
        self.owner = worker_id
        self.owner_writes.append((task_id, worker_id))

    async def get_task_state(self, *, task_id: str):
        return self.state

    async def set_task_state(self, *, task_id: str, state: str) -> None:
        self.state = state
        self.state_writes.append((task_id, state))

    async def set_task_output(self, *, task_id: str, record: dict) -> None:
        return None


class FakeEventStore:
    def __init__(self, results: list[bool] | None = None) -> None:
        self.results = list(results or [])
        self.events: list[dict] = []

    async def append_event(self, *, run_id, event_type, worker_id=None, details=None):
        self.events.append(
            {
                "run_id": run_id,
                "event_type": event_type,
                "worker_id": worker_id,
                "details": details or {},
            }
        )
        if self.results:
            return self.results.pop(0)
        return True


@dataclass
class FakeReceipt:
    duplicate: bool
    owner_id: str = "worker-1"
    reason: str = ""
    committed_state: str | None = None


class DuplicateEffectLedger:
    async def acquire_effect(self, **kwargs):
        return FakeReceipt(duplicate=True)


@pytest.fixture(autouse=True)
def completion_slice_flag(monkeypatch):
    monkeypatch.setenv("IRON_V3_COMPLETION_SLICE", "1")
    monkeypatch.delenv("IRON_V3_EVENT_GATE", raising=False)


def make_store(control: FakeControlStore, events: FakeEventStore, *, effect_ledger=None):
    return StateStore(
        redis_client=object(),
        lua_executor=object(),
        event_store=events,
        effect_ledger=effect_ledger,
        control_store=control,
    )


@pytest.mark.asyncio
async def test_completion_slice_appends_request_and_final_before_projection():
    control = FakeControlStore(owner="worker-1", state=None)
    events = FakeEventStore()
    store = make_store(control, events)

    result = await store.complete_once(
        run_id="run-1",
        task_id="task-1",
        worker_id="worker-1",
        status="done",
        attempt=1,
    )

    assert result.ok is True
    assert control.state_writes == [("task-1", "done")]
    assert [event["event_type"] for event in events.events] == [
        TASK_COMPLETION_REQUESTED,
        TASK_COMPLETED,
    ]


@pytest.mark.asyncio
async def test_completion_slice_blocks_projection_when_final_event_append_fails():
    control = FakeControlStore(owner="worker-1", state=None)
    events = FakeEventStore(results=[True, False])
    store = make_store(control, events)

    result = await store.complete_once(
        run_id="run-2",
        task_id="task-2",
        worker_id="worker-1",
        status="done",
        attempt=1,
    )

    assert result.ok is False
    assert result.status == "event_gate_blocked"
    assert control.state_writes == []
    assert control.owner_writes == []
    assert [event["event_type"] for event in events.events] == [
        TASK_COMPLETION_REQUESTED,
        TASK_COMPLETED,
    ]


@pytest.mark.asyncio
async def test_completion_slice_blocks_projection_when_request_event_fails():
    control = FakeControlStore(owner="worker-1", state=None)
    events = FakeEventStore(results=[False])
    store = make_store(control, events)

    result = await store.complete_once(
        run_id="run-3",
        task_id="task-3",
        worker_id="worker-1",
        status="failed",
        attempt=1,
    )

    assert result.ok is False
    assert result.status == "completion_request_event_blocked"
    assert control.state_writes == []
    assert [event["event_type"] for event in events.events] == [
        TASK_COMPLETION_REQUESTED,
    ]


@pytest.mark.asyncio
async def test_stale_owner_is_fenced_before_completion_events():
    control = FakeControlStore(owner="worker-current", state=None)
    events = FakeEventStore()
    store = make_store(control, events)

    result = await store.complete_once(
        run_id="run-4",
        task_id="task-4",
        worker_id="worker-stale",
        status="done",
        attempt=1,
    )

    assert result.ok is False
    assert result.status == "stale_owner_fenced"
    assert events.events == []
    assert control.state_writes == []


@pytest.mark.asyncio
async def test_duplicate_completion_is_suppressed_before_completion_events():
    control = FakeControlStore(owner="worker-1", state=None)
    events = FakeEventStore()
    store = make_store(control, events, effect_ledger=DuplicateEffectLedger())

    result = await store.complete_once(
        run_id="run-5",
        task_id="task-5",
        worker_id="worker-1",
        status="done",
        attempt=1,
    )

    assert result.ok is False
    assert result.duplicate is True
    assert result.status == "duplicate_completion_suppressed"
    assert events.events == []
    assert control.state_writes == []


def test_completion_slice_replay_only_terminal_after_final_event():
    requested_only = replay_completion_slice(
        task_id="task-6",
        events=[{"event_type": TASK_COMPLETION_REQUESTED, "run_id": "run-6"}],
    )
    assert requested_only.terminal is False
    assert requested_only.final_state is None

    completed = replay_completion_slice(
        task_id="task-6",
        events=[
            {"event_type": TASK_COMPLETION_REQUESTED, "run_id": "run-6"},
            {"event_type": TASK_COMPLETED, "run_id": "run-6", "worker_id": "worker-1"},
        ],
    )
    assert completed.terminal is True
    assert completed.final_state == "done"
    assert completed.worker_id == "worker-1"

    failed = replay_completion_slice(
        task_id="task-7",
        events=[{"event_type": TASK_FAILED, "run_id": "run-7"}],
    )
    assert failed.terminal is True
    assert failed.final_state == "failed"
