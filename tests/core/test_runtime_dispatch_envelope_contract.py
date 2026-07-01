from __future__ import annotations

import json
from pathlib import Path

from hfa.events.codec import deserialize_run_requested, serialize_event
from hfa.events.schema import RunRequestedEvent


def test_run_requested_event_carries_scheduler_epoch_as_envelope_metadata():
    event = RunRequestedEvent(
        run_id="run-envelope-1",
        tenant_id="tenant-a",
        agent_type="fake",
        payload={"prompt": "hello"},
        scheduler_epoch="epoch-envelope-1",
    )

    encoded = serialize_event(event)

    assert encoded["scheduler_epoch"] == "epoch-envelope-1"
    assert json.loads(encoded["payload"]) == {"prompt": "hello"}
    assert "scheduler_epoch" not in json.loads(encoded["payload"])


def test_deserialize_run_requested_preserves_scheduler_epoch_from_stream_field():
    event = deserialize_run_requested(
        {
            b"run_id": b"run-envelope-2",
            b"tenant_id": b"tenant-a",
            b"agent_type": b"fake",
            b"payload_json": b"{\"prompt\":\"hello\"}",
            b"scheduler_epoch": b"epoch-envelope-2",
        }
    )

    assert event.scheduler_epoch == "epoch-envelope-2"
    assert event.payload == {"prompt": "hello"}


def test_scheduler_lua_fallback_writes_scheduler_epoch_to_stream_envelope_not_payload():
    source = Path("hfa-control/src/hfa_control/scheduler_lua.py").read_text(encoding="utf-8")

    assert "scheduler_epoch: str = \"\"" in source
    assert "\"scheduler_epoch\": scheduler_epoch or \"\"" in source
    assert source.count("\"scheduler_epoch\": scheduler_epoch or \"\"") >= 3


def test_worker_consumer_uses_deserialize_run_requested_for_runtime_envelope():
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")

    assert "event = deserialize_run_requested(data)" in source
    assert "event.payload = decoded_payload" in source
    assert "scheduler_epoch" not in "payload_json"
