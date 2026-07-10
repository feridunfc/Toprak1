from __future__ import annotations

from hfa.events.codec import deserialize_run_requested, serialize_event
from hfa.events.schema import RunRequestedEvent


def test_run_requested_event_declares_first_class_task_id() -> None:
    event = RunRequestedEvent(
        task_id="task-76-explicit",
        run_id="run-76-explicit",
    )

    assert event.task_id == "task-76-explicit"
    assert event.run_id == "run-76-explicit"


def test_run_requested_serializer_preserves_task_id_and_run_id_separately() -> None:
    event = RunRequestedEvent(
        task_id="task-76-serialize",
        run_id="run-76-serialize",
        tenant_id="tenant-76",
        payload={"prompt": "identity propagation"},
    )

    fields = serialize_event(event)

    assert fields["task_id"] == "task-76-serialize"
    assert fields["run_id"] == "run-76-serialize"
    assert fields["task_id"] != fields["run_id"]


def test_run_requested_deserializer_parses_task_id_and_run_id_separately() -> None:
    event = deserialize_run_requested(
        {
            b"task_id": b"task-76-deserialize",
            b"run_id": b"run-76-deserialize",
            b"tenant_id": b"tenant-76",
            b"agent_type": b"agent-76",
        }
    )

    assert event.task_id == "task-76-deserialize"
    assert event.run_id == "run-76-deserialize"
    assert event.task_id != event.run_id


def test_legacy_run_only_payload_keeps_task_id_empty() -> None:
    event = deserialize_run_requested(
        {
            b"run_id": b"legacy-run-only-76",
            b"tenant_id": b"tenant-legacy",
        }
    )

    assert event.task_id == ""
    assert event.run_id == "legacy-run-only-76"


def test_equal_task_id_and_run_id_values_are_allowed_when_both_are_explicit() -> None:
    event = RunRequestedEvent(
        task_id="shared-explicit-id-76",
        run_id="shared-explicit-id-76",
    )

    fields = serialize_event(event)

    assert event.task_id == "shared-explicit-id-76"
    assert event.run_id == "shared-explicit-id-76"
    assert fields["task_id"] == "shared-explicit-id-76"
    assert fields["run_id"] == "shared-explicit-id-76"
