from __future__ import annotations

from pathlib import Path

import pytest

from hfa.config.keys import RedisKey
from hfa_control.run_terminal_event_evidence import (
    CONTRACT_PRODUCER_FIELD,
    CONTRACT_SCHEMA_FIELD,
    READINESS_RESULTS_STREAM_FIELD,
    READINESS_SOURCE_HISTORY_FIELD,
    READINESS_STATUS_FIELD,
    TerminalEventEvidenceError,
    evidence_from_stream_row,
    stream_history_snapshot,
    validate_index_contract,
    validate_readiness,
)


def test_key_contract_is_versioned_deterministic_and_coevicts_readiness_with_evidence():
    assert RedisKey.run_terminal_event_index() == "hfa:run:terminal-event:v1:index"
    assert RedisKey.run_terminal_event_evidence_field("tenant-a:run-1") == "run:tenant-a:run-1"
    assert "migration-readiness" not in RedisKey.run_terminal_event_index()


def test_index_contract_is_explicit_and_fail_closed():
    contract = {
        CONTRACT_SCHEMA_FIELD: "1",
        CONTRACT_PRODUCER_FIELD: "1",
    }
    assert validate_index_contract(contract) == contract
    with pytest.raises(TerminalEventEvidenceError, match="producer contract"):
        validate_index_contract({CONTRACT_SCHEMA_FIELD: "1"})


def test_history_snapshot_requires_untrimmed_undeleted_history():
    snapshot = stream_history_snapshot(
        {
            "length": 5,
            "entries-added": 5,
            "max-deleted-entry-id": "0-0",
            "last-generated-id": "100-0",
        }
    )
    assert snapshot.length == snapshot.entries_added == 5
    with pytest.raises(TerminalEventEvidenceError, match="trimming/deletion"):
        stream_history_snapshot(
            {
                "length": 4,
                "entries-added": 5,
                "max-deleted-entry-id": "0-0",
                "last-generated-id": "100-0",
            }
        )
    with pytest.raises(TerminalEventEvidenceError, match="trimming/deletion"):
        stream_history_snapshot(
            {
                "length": 5,
                "entries-added": 5,
                "max-deleted-entry-id": "90-0",
                "last-generated-id": "100-0",
            }
        )


def test_historical_terminal_event_contract_and_malformed_evidence():
    completed = evidence_from_stream_row(
        "10-0",
        {
            "event_id": "evt-1",
            "event_type": "RunCompleted",
            "run_id": "run-1",
            "tenant_id": "tenant-a",
        },
    )
    assert completed == {
        "schema_version": "1",
        "run_id": "run-1",
        "tenant_id": "tenant-a",
        "event_type": "RunCompleted",
        "final_state": "done",
        "event_id": "evt-1",
        "source": "historical_backfill",
        "stream_entry_id": "10-0",
    }
    assert evidence_from_stream_row("11-0", {"event_type": "TaskCompleted"}) is None
    with pytest.raises(TerminalEventEvidenceError, match="malformed"):
        evidence_from_stream_row(
            "12-0",
            {"event_type": "RunFailed", "run_id": "run-1", "tenant_id": "tenant-a"},
        )


def test_readiness_contract_is_explicit_and_fail_closed():
    ready = {
        CONTRACT_SCHEMA_FIELD: "1",
        CONTRACT_PRODUCER_FIELD: "1",
        READINESS_STATUS_FIELD: "ready",
        READINESS_RESULTS_STREAM_FIELD: RedisKey.stream_results(),
        READINESS_SOURCE_HISTORY_FIELD: "1",
    }
    assert validate_readiness(ready)[READINESS_STATUS_FIELD] == "ready"
    with pytest.raises(TerminalEventEvidenceError, match="readiness"):
        validate_readiness({**ready, READINESS_SOURCE_HISTORY_FIELD: "0"})
    with pytest.raises(TerminalEventEvidenceError, match="producer contract"):
        validate_readiness({**ready, CONTRACT_PRODUCER_FIELD: "2"})


def test_canonical_projection_has_no_unbounded_results_scan_and_uses_shared_index():
    source = Path("hfa-core/src/hfa/lua/run_terminate_projection.lua").read_text()
    normalized = " ".join(source.split())
    assert 'XRANGE", results_stream, "-", "+"' not in normalized
    assert 'XREVRANGE", results_stream, "+", "-"' not in normalized
    assert 'redis.call("SCAN"' not in source
    assert 'redis.call("KEYS"' not in source
    assert 'XRANGE", results_stream, stream_entry_id, stream_entry_id' in normalized
    assert "terminal_event_migration_not_ready" in source
    assert '"HSET", terminal_event_index_key, evidence_field, evidence_json' in normalized
    assert '"__migration__:status"' in source
    assert '"__contract__:producer_contract_version"' in source


def test_legacy_lua_writes_event_and_evidence_in_same_atomic_script():
    source = Path("hfa-core/src/hfa/lua/run_terminate_from_tasks.lua").read_text()
    assert '"XADD"' in source
    assert 'redis.call("HSET", terminal_event_index_key, evidence_field, evidence_json)' in source
    assert 'redis.call("PERSIST", terminal_event_index_key)' in source
    assert "terminal_event_index_missing_or_wrong_type" in source


def test_modified_legacy_lua_is_packaged_in_hfa_core_wheel():
    source = Path("hfa-core/pyproject.toml").read_text()
    assert '"lua/run_terminate_from_tasks.lua"' in source


def test_legacy_retry_accepts_exact_historical_backfill_evidence():
    source = Path("hfa-core/src/hfa/lua/run_terminate_from_tasks.lua").read_text()
    assert 'evidence_source ~= "legacy_run_terminate" and evidence_source ~= "historical_backfill"' in source
