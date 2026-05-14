"""
tests/core/test_sprint12_backward_compat_events.py
IRONCLAD Sprint 12 — Schema/codec backward compatibility tests

Verifies:
  - Old stream payloads (missing new optional fields) parse cleanly
  - Unknown/extra fields in payloads are silently ignored
  - Existing event class names unchanged
  - Existing event field names unchanged
  - deserialize_run_completed and deserialize_run_failed handle missing fields
  - RunStartedEvent is additive (new in Sprint 12) and doesn't affect existing events
"""

from __future__ import annotations


from hfa.events.codec import (
    deserialize_run_completed,
    deserialize_run_failed,
    deserialize_run_requested,
    serialize_event,
)
from hfa.events.schema import (
    GraphPatchedEvent,
    RunAdmittedEvent,
    RunCompletedEvent,
    RunDeadLetteredEvent,
    RunFailedEvent,
    RunRescheduledEvent,
    RunRequestedEvent,
    RunScheduledEvent,
    RunStartedEvent,
    WorkerDrainingEvent,
    WorkerHeartbeatEvent,
)


# ---------------------------------------------------------------------------
# Existing event names must not change
# ---------------------------------------------------------------------------


def test_existing_event_class_names_unchanged():
    """All Sprint 9/10/11 event class names must be stable."""
    assert RunRequestedEvent.__name__ == "RunRequestedEvent"
    assert RunCompletedEvent.__name__ == "RunCompletedEvent"
    assert RunFailedEvent.__name__ == "RunFailedEvent"
    assert RunAdmittedEvent.__name__ == "RunAdmittedEvent"
    assert RunScheduledEvent.__name__ == "RunScheduledEvent"
    assert RunRescheduledEvent.__name__ == "RunRescheduledEvent"
    assert RunDeadLetteredEvent.__name__ == "RunDeadLetteredEvent"
    assert WorkerHeartbeatEvent.__name__ == "WorkerHeartbeatEvent"
    assert WorkerDrainingEvent.__name__ == "WorkerDrainingEvent"
    assert GraphPatchedEvent.__name__ == "GraphPatchedEvent"


def test_run_requested_required_fields_unchanged():
    e = RunRequestedEvent(run_id="r1", tenant_id="t1", agent_type="base")
    assert e.run_id == "r1"
    assert e.tenant_id == "t1"
    assert e.agent_type == "base"
    assert e.event_type == "RunRequested"


def test_run_completed_required_fields_unchanged():
    e = RunCompletedEvent(run_id="r1", tenant_id="t1", worker_id="w1", cost_cents=5)
    assert e.run_id == "r1"
    assert e.cost_cents == 5


# ---------------------------------------------------------------------------
# deserialize_run_requested: missing optional fields → safe defaults
# ---------------------------------------------------------------------------


def test_deserialize_run_requested_minimal_payload():
    """Old payload missing idempotency_key, payload, trace fields."""
    data: dict = {
        b"run_id": b"r-old-1",
        b"tenant_id": b"t-old",
        b"agent_type": b"base",
    }
    event = deserialize_run_requested(data)
    assert event.run_id == "r-old-1"
    assert event.tenant_id == "t-old"
    assert event.idempotency_key == ""
    assert event.payload == {}
    assert event.trace_parent is None


def test_deserialize_run_requested_unknown_fields_tolerated():
    """Extra fields in payload must not break parsing."""
    data: dict = {
        b"run_id": b"r-new-1",
        b"tenant_id": b"t1",
        b"agent_type": b"base",
        b"future_field_v3": b"some_value",
        b"experimental_flag": b"true",
    }
    event = deserialize_run_requested(data)
    assert event.run_id == "r-new-1"


def test_deserialize_run_requested_priority_defaults_to_5():
    data: dict = {b"run_id": b"r1", b"tenant_id": b"t1"}
    event = deserialize_run_requested(data)
    assert event.priority == 5


# ---------------------------------------------------------------------------
# deserialize_run_completed: Sprint 12 addition — tolerates missing fields
# ---------------------------------------------------------------------------


def test_deserialize_run_completed_full_payload():
    data: dict = {
        b"run_id": b"r1",
        b"tenant_id": b"t1",
        b"worker_id": b"w1",
        b"cost_cents": b"42",
        b"tokens_used": b"100",
        b"payload": b'{"result": "ok"}',
    }
    event = deserialize_run_completed(data)
    assert event.run_id == "r1"
    assert event.cost_cents == 42
    assert event.tokens_used == 100
    assert event.payload == {"result": "ok"}


def test_deserialize_run_completed_minimal_payload():
    data: dict = {b"run_id": b"r1", b"tenant_id": b"t1"}
    event = deserialize_run_completed(data)
    assert event.run_id == "r1"
    assert event.cost_cents == 0
    assert event.tokens_used == 0


def test_deserialize_run_completed_unknown_fields_tolerated():
    data: dict = {
        b"run_id": b"r1",
        b"tenant_id": b"t1",
        b"future_field": b"x",
    }
    event = deserialize_run_completed(data)
    assert event.run_id == "r1"


# ---------------------------------------------------------------------------
# deserialize_run_failed: Sprint 12 addition — tolerates missing fields
# ---------------------------------------------------------------------------


def test_deserialize_run_failed_full_payload():
    data: dict = {
        b"run_id": b"r1",
        b"tenant_id": b"t1",
        b"worker_id": b"w1",
        b"error": b"timeout",
        b"cost_cents": b"5",
    }
    event = deserialize_run_failed(data)
    assert event.run_id == "r1"
    assert event.error == "timeout"
    assert event.cost_cents == 5


def test_deserialize_run_failed_minimal():
    data: dict = {b"run_id": b"r1", b"tenant_id": b"t1"}
    event = deserialize_run_failed(data)
    assert event.run_id == "r1"
    assert event.error == ""


# ---------------------------------------------------------------------------
# RunStartedEvent — Sprint 12 additive event
# ---------------------------------------------------------------------------


def test_run_started_event_is_additive():
    e = RunStartedEvent(
        run_id="r1",
        tenant_id="t1",
        worker_id="w1",
        worker_group="grp",
        shard=0,
    )
    assert e.event_type == "RunStarted"
    assert e.run_id == "r1"
    assert e.shard == 0


def test_run_started_event_serializes():
    e = RunStartedEvent(run_id="r1", tenant_id="t1", worker_id="w1")
    serialized = serialize_event(e)
    assert serialized["event_type"] == "RunStarted"
    assert serialized["run_id"] == "r1"


def test_run_started_event_default_fields():
    e = RunStartedEvent()
    assert e.run_id == ""
    assert e.shard == 0
    assert e.worker_group == ""


# ---------------------------------------------------------------------------
# Round-trip: serialize then deserialize
# ---------------------------------------------------------------------------


def test_serialize_deserialize_run_requested_roundtrip():
    original = RunRequestedEvent(
        run_id="r-rt-1",
        tenant_id="t1",
        agent_type="base",
        priority=3,
        payload={"x": 1},
        idempotency_key="idem-1",
    )
    serialized = serialize_event(original)
    # convert to bytes-keyed dict as Redis would give us
    redis_data = {k.encode(): v.encode() for k, v in serialized.items()}
    recovered = deserialize_run_requested(redis_data)

    assert recovered.run_id == original.run_id
    assert recovered.tenant_id == original.tenant_id
    assert recovered.priority == original.priority
    assert recovered.payload == original.payload
    assert recovered.idempotency_key == original.idempotency_key
