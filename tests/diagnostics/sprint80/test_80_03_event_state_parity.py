from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

import hfa_control.admission as admission_module
from hfa.config.keys import RedisKey
from hfa.runtime.state_store import StateStore
from hfa_control.admission import AdmissionController
from hfa_control.event_store import EventStore
from hfa_core.events.event_types import TASK_COMPLETED


class _RecordingEventStore:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def append_event(
        self,
        *,
        run_id: str,
        event_type: str,
        worker_id: str | None = None,
        details: dict | None = None,
    ) -> bool:
        self.events.append(
            {
                "run_id": run_id,
                "event_type": event_type,
                "worker_id": worker_id,
                "details": dict(details or {}),
            }
        )
        return True


class _FailingEventStore:
    def __init__(self) -> None:
        self.calls = 0

    async def append_event(self, **kwargs) -> bool:
        self.calls += 1
        return False


class _FailingTaskControlStore:
    async def get_owner(self, *, task_id: str):
        return None

    async def get_task_state(self, *, task_id: str):
        return None

    async def set_task_state(self, *, task_id: str, state: str):
        raise RuntimeError("forced_state_write_failure")

    async def set_owner(self, *, task_id: str, worker_id: str):
        raise AssertionError("owner write must not run after failed state mutation")


class _FailingXAddRedis:
    def __init__(self, delegate) -> None:
        self._delegate = delegate
        self.xadd_attempts = 0

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    async def xadd(self, *args, **kwargs):
        self.xadd_attempts += 1
        raise RuntimeError("forced_transport_failure")


async def _observe_event_first_orphan(sprint80_redis, monkeypatch):
    monkeypatch.setenv("IRON_V3_COMPLETION_SLICE", "1")
    event_store = _RecordingEventStore()
    store = StateStore(
        sprint80_redis,
        event_store=event_store,
        control_store=_FailingTaskControlStore(),
    )

    with pytest.raises(RuntimeError, match="forced_state_write_failure"):
        await store.complete_once(
            run_id="s80-run-event-first",
            task_id="s80-task-event-first",
            worker_id="s80-worker",
            status="done",
        )

    return event_store.events


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_event_first_append_can_leave_terminal_orphan_event(
    sprint80_redis,
    monkeypatch,
):
    events = await _observe_event_first_orphan(sprint80_redis, monkeypatch)
    event_types = [event["event_type"] for event in events]

    assert TASK_COMPLETED in event_types
    assert await sprint80_redis.get("hfa:dag:task:s80-task-event-first:state") is None


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_real_eventstore_append_survives_forced_state_failure(
    sprint80_redis,
    monkeypatch,
):
    monkeypatch.setenv("IRON_V3_EVENT_GATE", "1")
    monkeypatch.delenv("IRON_V3_COMPLETION_SLICE", raising=False)
    run_id = "s80-run-real-eventstore"
    task_id = "s80-task-real-eventstore"
    event_store = EventStore(sprint80_redis)
    store = StateStore(
        sprint80_redis,
        event_store=event_store,
        control_store=_FailingTaskControlStore(),
    )

    with pytest.raises(RuntimeError, match="forced_state_write_failure"):
        await store.complete_once(
            run_id=run_id,
            task_id=task_id,
            worker_id="s80-worker",
            status="done",
        )

    assert await sprint80_redis.llen(EventStore.event_key(run_id)) == 1
    raw = await sprint80_redis.lindex(EventStore.event_key(run_id), 0)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    event = json.loads(raw)
    assert event["event_type"] == TASK_COMPLETED
    assert await sprint80_redis.get(f"hfa:dag:task:{task_id}:state") is None


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_state_first_admission_can_leave_missing_transport(
    sprint80_redis,
    monkeypatch,
):
    monkeypatch.setattr(admission_module, "QuotaManager", None)
    monkeypatch.setattr(admission_module, "validate_run_id_format", None)

    redis_proxy = _FailingXAddRedis(sprint80_redis)
    controller = AdmissionController(
        redis_proxy,
        SimpleNamespace(control_stream="s80:stream:control"),
        tenant_registry=None,
        rate_limiter=None,
    )
    request = SimpleNamespace(
        run_id="s80-run-state-first",
        tenant_id="s80-tenant",
        agent_type="test",
        priority=0,
        payload={"purpose": "sprint80 parity probe"},
        estimated_cost_cents=0,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
    )

    with pytest.raises(RuntimeError, match="forced_transport_failure"):
        await controller.admit(request)

    raw_state = await sprint80_redis.get(RedisKey.run_state(request.run_id))
    state = raw_state.decode() if isinstance(raw_state, bytes) else raw_state
    assert state == "admitted"
    assert redis_proxy.xadd_attempts == 1
    assert await sprint80_redis.xlen("s80:stream:control") == 0


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_committed_state_survives_background_audit_failure(
    sprint80_redis,
    monkeypatch,
):
    monkeypatch.delenv("IRON_V3_EVENT_GATE", raising=False)
    monkeypatch.delenv("IRON_V3_COMPLETION_SLICE", raising=False)
    event_store = _FailingEventStore()
    store = StateStore(sprint80_redis, event_store=event_store)
    task_id = "s80-task-background-audit"

    result = await store.complete_once(
        run_id="s80-run-background-audit",
        task_id=task_id,
        worker_id="s80-worker",
        status="done",
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    raw_state = await sprint80_redis.get(f"hfa:dag:task:{task_id}:state")
    state = raw_state.decode() if isinstance(raw_state, bytes) else raw_state
    assert result.ok is True
    assert state == "done"
    assert event_store.calls == 1


@pytest.mark.asyncio
@pytest.mark.sprint80_contract
@pytest.mark.sprint80_real_redis
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Final event append and canonical state mutation are not one local transaction; "
        "a terminal event may remain after state mutation fails"
    ),
)
async def test_terminal_transition_record_cannot_exist_without_committed_state(
    sprint80_redis,
    monkeypatch,
):
    events = await _observe_event_first_orphan(sprint80_redis, monkeypatch)
    assert TASK_COMPLETED not in [event["event_type"] for event in events]


@pytest.mark.asyncio
@pytest.mark.sprint80_contract
@pytest.mark.sprint80_real_redis
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Legacy background audit append may fail after state commit; no atomic parity or "
        "durable recovery contract currently repairs the missing audit record"
    ),
)
async def test_committed_terminal_state_requires_durable_audit_record(
    sprint80_redis,
    monkeypatch,
):
    monkeypatch.delenv("IRON_V3_EVENT_GATE", raising=False)
    monkeypatch.delenv("IRON_V3_COMPLETION_SLICE", raising=False)
    event_store = _FailingEventStore()
    store = StateStore(sprint80_redis, event_store=event_store)
    task_id = "s80-task-required-audit"

    result = await store.complete_once(
        run_id="s80-run-required-audit",
        task_id=task_id,
        worker_id="s80-worker",
        status="done",
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    raw_state = await sprint80_redis.get(f"hfa:dag:task:{task_id}:state")
    assert result.ok is not True or raw_state is None or event_store.calls == 0
