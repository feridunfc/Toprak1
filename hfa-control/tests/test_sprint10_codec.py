"""
hfa-control/tests/test_sprint10_codec.py
IRONCLAD Sprint 10 — Event codec tests
"""

from __future__ import annotations

import json
import pytest

from hfa.events.codec import serialize_event, decode_field
from hfa.events.schema import (
    RunAdmittedEvent,
    WorkerHeartbeatEvent,
    RunDeadLetteredEvent,
)


class TestSerializeEvent:
    def test_basic_fields(self):
        evt = RunAdmittedEvent(run_id="r1", tenant_id="acme", agent_type="coder")
        data = serialize_event(evt)
        assert data["run_id"] == "r1"
        assert data["tenant_id"] == "acme"
        assert data["event_type"] == "RunAdmitted"

    def test_list_serialised_as_json(self):
        evt = WorkerHeartbeatEvent(worker_id="w1", shards=[0, 1, 2])
        data = serialize_event(evt)
        assert json.loads(data["shards"]) == [0, 1, 2]

    def test_dict_serialised_as_json(self):
        evt = RunAdmittedEvent(run_id="r2", payload={"code": "print(1)"})
        data = serialize_event(evt)
        assert json.loads(data["payload"])["code"] == "print(1)"

    def test_int_as_string(self):
        evt = RunAdmittedEvent(run_id="r3", estimated_cost_cents=500)
        data = serialize_event(evt)
        assert data["estimated_cost_cents"] == "500"

    def test_none_trace_fields_omitted(self):
        evt = RunAdmittedEvent(run_id="r4")
        data = serialize_event(evt)
        assert "trace_parent" not in data
        assert "trace_state" not in data

    def test_non_dataclass_raises(self):
        with pytest.raises(TypeError):
            serialize_event({"not": "a dataclass"})

    def test_cost_cents_is_int_not_float(self):
        evt = RunDeadLetteredEvent(run_id="r5", cost_cents=250)
        data = serialize_event(evt)
        # Must be string repr of int, not float
        assert data["cost_cents"] == "250"
        assert "." not in data["cost_cents"]


class TestDecodeField:
    def test_decode_int(self):
        assert decode_field("inflight", b"7", "int") == 7

    def test_decode_float(self):
        result = decode_field("timestamp", b"1.5", "float")
        assert abs(result - 1.5) < 0.001

    def test_decode_list(self):
        result = decode_field("shards", b"[0,1,2]", "List[int]")
        assert result == [0, 1, 2]

    def test_decode_dict(self):
        result = decode_field("payload", b'{"k":"v"}', "Dict[str, Any]")
        assert result == {"k": "v"}

    def test_decode_str(self):
        assert decode_field("run_id", b"r1", "str") == "r1"

    def test_decode_empty_returns_default_int(self):
        assert decode_field("cost_cents", b"", "int") == 0

    def test_decode_empty_returns_default_list(self):
        assert decode_field("shards", b"", "List[int]") == []

    def test_decode_corrupt_json_returns_default(self):
        result = decode_field("shards", b"NOT_JSON", "List[int]")
        assert result == []

    def test_decode_corrupt_int_returns_zero(self):
        assert decode_field("inflight", b"abc", "int") == 0

    def test_decode_bool_true(self):
        assert decode_field("flag", b"true", "bool") is True

    def test_decode_bool_false(self):
        assert decode_field("flag", b"false", "bool") is False
