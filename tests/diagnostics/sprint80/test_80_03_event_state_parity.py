from __future__ import annotations

from types import SimpleNamespace

import pytest

import hfa_control.admission as admission_module
from hfa.config.keys import RedisKey
from hfa.runtime.state_store import StateStore
from hfa_control.admission import AdmissionController
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
