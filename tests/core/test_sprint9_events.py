from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent


def test_serialize_event_preserves_none_as_empty_string():
    event = RunRequestedEvent(run_id="r1", tenant_id="t1", trace_parent=None)
    fields = serialize_event(event)
    assert "trace_parent" in fields
    assert fields["trace_parent"] == ""
