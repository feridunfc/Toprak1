"""Sprint 84.6 durable RUN terminal-event evidence and bounded migration.

The evidence and migration-readiness state deliberately share one Redis HASH.
That coupling is a safety property: if Redis eviction/data loss removes the
index, readiness disappears with the evidence and all patched terminal writers
fail closed instead of interpreting an absent per-RUN record as proof of
non-existence.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from hfa.config.keys import RedisKey

SCHEMA_VERSION = "1"
PRODUCER_CONTRACT_VERSION = "1"
READY_STATUS = "ready"
TERMINAL_EVENT_TYPES = frozenset({"RunCompleted", "RunFailed"})
TERMINAL_EVIDENCE_SOURCES = frozenset({
    "historical_backfill",
    "legacy_run_terminate",
    "canonical_run_terminate",
    "worker_consumer_compat",
})

CONTRACT_SCHEMA_FIELD = "__contract__:schema_version"
CONTRACT_PRODUCER_FIELD = "__contract__:producer_contract_version"
READINESS_STATUS_FIELD = "__migration__:status"
READINESS_RESULTS_STREAM_FIELD = "__migration__:results_stream_key"
READINESS_SOURCE_HISTORY_FIELD = "__migration__:source_history_complete"
READINESS_SNAPSHOT_LAST_ID_FIELD = "__migration__:snapshot_last_entry_id"
READINESS_ENTRIES_ADDED_FIELD = "__migration__:entries_added"
READINESS_SCANNED_FIELD = "__migration__:scanned_entries"
READINESS_TERMINAL_EVENTS_FIELD = "__migration__:terminal_events"
READINESS_EVIDENCE_CREATED_FIELD = "__migration__:evidence_created"
READINESS_COMPLETED_AT_FIELD = "__migration__:completed_at_ms"

CONTRACT_FIELDS = (CONTRACT_SCHEMA_FIELD, CONTRACT_PRODUCER_FIELD)
READINESS_FIELDS = (
    READINESS_STATUS_FIELD,
    READINESS_RESULTS_STREAM_FIELD,
    READINESS_SOURCE_HISTORY_FIELD,
    READINESS_SNAPSHOT_LAST_ID_FIELD,
    READINESS_ENTRIES_ADDED_FIELD,
    READINESS_SCANNED_FIELD,
    READINESS_TERMINAL_EVENTS_FIELD,
    READINESS_EVIDENCE_CREATED_FIELD,
    READINESS_COMPLETED_AT_FIELD,
)


class TerminalEventEvidenceError(RuntimeError):
    pass


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return "" if value is None else str(value)


def _mapping(raw: Mapping[Any, Any]) -> dict[str, str]:
    return {_text(k): _text(v) for k, v in raw.items()}


def _info_value(info: Mapping[Any, Any], *names: str) -> Any:
    for name in names:
        if name in info:
            return info[name]
        encoded = name.encode("utf-8")
        if encoded in info:
            return info[encoded]
    return None


def _canonical_json(value: Mapping[str, str]) -> str:
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _decode_evidence(raw: Any) -> dict[str, str]:
    text = _text(raw)
    if not text:
        raise TerminalEventEvidenceError("terminal-event evidence is missing")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise TerminalEventEvidenceError("terminal-event evidence JSON is malformed") from exc
    if not isinstance(value, dict) or any(type(k) is not str or type(v) is not str for k, v in value.items()):
        raise TerminalEventEvidenceError("terminal-event evidence JSON has invalid shape")
    required = {"schema_version", "run_id", "tenant_id", "event_type", "final_state", "event_id", "source"}
    if not required.issubset(value):
        raise TerminalEventEvidenceError("terminal-event evidence fields are incomplete")
    if value["schema_version"] != SCHEMA_VERSION:
        raise TerminalEventEvidenceError("terminal-event evidence schema version is invalid")
    if value["event_type"] not in TERMINAL_EVENT_TYPES:
        raise TerminalEventEvidenceError("terminal-event evidence type is invalid")
    expected_state = "done" if value["event_type"] == "RunCompleted" else "failed"
    if value["final_state"] != expected_state:
        raise TerminalEventEvidenceError("terminal-event evidence state is inconsistent")
    if not value["run_id"] or not value["tenant_id"] or not value["event_id"] or not value["source"]:
        raise TerminalEventEvidenceError("terminal-event evidence identity is incomplete")
    if value["source"] not in TERMINAL_EVIDENCE_SOURCES:
        raise TerminalEventEvidenceError("terminal-event evidence source is invalid")
    return value


def _contract_mapping() -> dict[str, str]:
    return {
        CONTRACT_SCHEMA_FIELD: SCHEMA_VERSION,
        CONTRACT_PRODUCER_FIELD: PRODUCER_CONTRACT_VERSION,
    }


def validate_index_contract(raw: Mapping[Any, Any]) -> dict[str, str]:
    data = _mapping(raw)
    required = _contract_mapping()
    if any(data.get(field) != expected for field, expected in required.items()):
        raise TerminalEventEvidenceError("terminal-event index producer contract is invalid")
    return data


def validate_readiness(raw: Mapping[Any, Any]) -> dict[str, str]:
    data = validate_index_contract(raw)
    required = {
        READINESS_STATUS_FIELD: READY_STATUS,
        READINESS_RESULTS_STREAM_FIELD: RedisKey.stream_results(),
        READINESS_SOURCE_HISTORY_FIELD: "1",
    }
    if any(data.get(field) != expected for field, expected in required.items()):
        raise TerminalEventEvidenceError("terminal-event migration readiness is invalid")
    return data


@dataclass(frozen=True)
class StreamHistorySnapshot:
    length: int
    entries_added: int
    max_deleted_entry_id: str
    last_generated_id: str


@dataclass(frozen=True)
class BackfillResult:
    status: str
    scanned_entries: int
    terminal_events: int
    evidence_created: int
    snapshot_last_entry_id: str


def stream_history_snapshot(info: Mapping[Any, Any]) -> StreamHistorySnapshot:
    try:
        length = int(_info_value(info, "length"))
        entries_added = int(_info_value(info, "entries-added", "entries_added"))
    except (TypeError, ValueError) as exc:
        raise TerminalEventEvidenceError("results stream XINFO counters are unavailable") from exc
    max_deleted = _text(_info_value(info, "max-deleted-entry-id", "max_deleted_entry_id"))
    last_generated = _text(_info_value(info, "last-generated-id", "last_generated_id"))
    if length < 0 or entries_added < 0 or not max_deleted or not last_generated:
        raise TerminalEventEvidenceError("results stream XINFO history fields are incomplete")
    if entries_added != length or max_deleted != "0-0":
        raise TerminalEventEvidenceError(
            "historical results stream is not complete: trimming/deletion already occurred"
        )
    return StreamHistorySnapshot(
        length=length,
        entries_added=entries_added,
        max_deleted_entry_id=max_deleted,
        last_generated_id=last_generated,
    )


def evidence_from_stream_row(entry_id: Any, raw_fields: Mapping[Any, Any]) -> dict[str, str] | None:
    fields = _mapping(raw_fields)
    event_type = fields.get("event_type", "")
    if event_type not in TERMINAL_EVENT_TYPES:
        return None
    run_id = fields.get("run_id", "")
    tenant_id = fields.get("tenant_id", "")
    event_id = fields.get("event_id", "")
    if not run_id or not tenant_id or not event_id:
        raise TerminalEventEvidenceError("historical terminal event identity is malformed")
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
        "final_state": "done" if event_type == "RunCompleted" else "failed",
        "event_id": event_id,
        "source": "historical_backfill",
        "stream_entry_id": _text(entry_id),
    }


async def _index_fields(redis: Any, fields: tuple[str, ...]) -> dict[str, str]:
    values = await redis.hmget(RedisKey.run_terminal_event_index(), *fields)
    return {field: _text(value) for field, value in zip(fields, values) if value is not None}


async def ensure_terminal_event_index(redis: Any) -> str:
    """Create/validate the persistent producer-contract shell.

    This is an explicit deployment preparation step. Patched terminal writers
    refuse to create the index themselves so eviction of the whole index cannot
    be mistaken for a fresh deployment.
    """

    key = RedisKey.run_terminal_event_index()
    observed_type = _text(await redis.type(key))
    if observed_type not in {"none", "hash"}:
        raise TerminalEventEvidenceError("terminal-event index has wrong Redis type")
    if observed_type == "hash":
        validate_index_contract(await _index_fields(redis, CONTRACT_FIELDS))
        if int(await redis.ttl(key)) != -1:
            raise TerminalEventEvidenceError("terminal-event index must be persistent")
        return "already_prepared"

    pipeline_factory = getattr(redis, "pipeline", None)
    if not callable(pipeline_factory):
        raise TerminalEventEvidenceError("Redis transaction support is required")
    try:
        from redis.exceptions import WatchError
    except Exception as exc:  # pragma: no cover
        raise TerminalEventEvidenceError("redis-py WatchError support is required") from exc

    for _attempt in range(8):
        async with pipeline_factory(transaction=True) as pipe:
            try:
                await pipe.watch(key)
                if _text(await pipe.type(key)) != "none":
                    await pipe.unwatch()
                    return await ensure_terminal_event_index(redis)
                pipe.multi()
                pipe.hset(key, mapping=_contract_mapping())
                pipe.persist(key)
                await pipe.execute()
                return "prepared"
            except WatchError:
                continue
    raise TerminalEventEvidenceError("terminal-event index preparation conflicted repeatedly")


async def _validate_existing_index_evidence(
    redis: Any,
    *,
    seen_terminal_entries: Mapping[str, tuple[str, str, str]],
    page_size: int,
) -> None:
    """Boundedly validate every existing per-RUN evidence field before readiness.

    A prior/partially migrated index is not trusted merely because its producer
    contract is valid.  Every run:* field must parse, self-identify, and map to
    exactly one terminal event in the frozen stream snapshot.  This scan is
    migration-only and never runs on the online RUN_TERMINATE projection path.
    """

    index_key = RedisKey.run_terminal_event_index()
    cursor: int | str = 0
    while True:
        cursor, rows = await redis.hscan(
            index_key,
            cursor=cursor,
            match="run:*",
            count=page_size,
        )
        for raw_field, raw_value in rows.items():
            field = _text(raw_field)
            evidence = _decode_evidence(raw_value)
            run_id = evidence["run_id"]
            if field != RedisKey.run_terminal_event_evidence_field(run_id):
                raise TerminalEventEvidenceError("terminal-event evidence field identity is inconsistent")
            observed = seen_terminal_entries.get(run_id)
            if observed is None:
                raise TerminalEventEvidenceError(
                    f"terminal-event evidence has no event in the frozen results snapshot for run {run_id!r}"
                )
            _stream_entry_id, event_id, event_type = observed
            if evidence["event_id"] != event_id or evidence["event_type"] != event_type:
                raise TerminalEventEvidenceError(
                    f"terminal-event evidence contradicts the frozen results snapshot for run {run_id!r}"
                )
        if int(cursor) == 0:
            return


async def _upsert_historical_evidence(redis: Any, evidence: dict[str, str]) -> bool:
    index_key = RedisKey.run_terminal_event_index()
    if _text(await redis.type(index_key)) != "hash":
        raise TerminalEventEvidenceError("terminal-event index is unavailable")
    validate_index_contract(await _index_fields(redis, CONTRACT_FIELDS))
    if int(await redis.ttl(index_key)) != -1:
        raise TerminalEventEvidenceError("terminal-event index must be persistent")

    field = RedisKey.run_terminal_event_evidence_field(evidence["run_id"])
    encoded = _canonical_json(evidence)
    inserted = int(await redis.hsetnx(index_key, field, encoded))
    if inserted == 1:
        return True

    existing = _decode_evidence(await redis.hget(index_key, field))
    identity_fields = ("schema_version", "run_id", "tenant_id", "event_type", "final_state", "event_id")
    if any(existing.get(name) != evidence[name] for name in identity_fields):
        raise TerminalEventEvidenceError(
            f"contradictory terminal events exist for run {evidence['run_id']!r}"
        )
    return False


async def backfill_terminal_event_evidence(
    redis: Any,
    *,
    page_size: int = 500,
    completed_at_ms: int,
) -> BackfillResult:
    if type(page_size) is not int or page_size < 1 or page_size > 5000:
        raise ValueError("page_size must be an integer in [1, 5000]")
    if type(completed_at_ms) is not int or completed_at_ms < 0:
        raise ValueError("completed_at_ms must be a non-negative integer")

    await ensure_terminal_event_index(redis)
    index_key = RedisKey.run_terminal_event_index()
    readiness = await _index_fields(redis, CONTRACT_FIELDS + READINESS_FIELDS)
    if readiness.get(READINESS_STATUS_FIELD):
        current = validate_readiness(readiness)
        if int(await redis.ttl(index_key)) != -1:
            raise TerminalEventEvidenceError("terminal-event index must be persistent")
        return BackfillResult(
            status="already_ready",
            scanned_entries=int(current.get(READINESS_SCANNED_FIELD, "0")),
            terminal_events=int(current.get(READINESS_TERMINAL_EVENTS_FIELD, "0")),
            evidence_created=int(current.get(READINESS_EVIDENCE_CREATED_FIELD, "0")),
            snapshot_last_entry_id=current.get(READINESS_SNAPSHOT_LAST_ID_FIELD, "0-0"),
        )

    stream = RedisKey.stream_results()
    if _text(await redis.type(stream)) != "stream":
        raise TerminalEventEvidenceError("results stream must exist before readiness can be proven")
    start_info = await redis.xinfo_stream(stream)
    snapshot = stream_history_snapshot(start_info)
    upper = snapshot.last_generated_id

    scanned = 0
    terminals = 0
    created = 0
    seen_terminal_entries: dict[str, tuple[str, str, str]] = {}
    cursor = "-"
    while True:
        rows = await redis.xrange(stream, min=cursor, max=upper, count=page_size)
        if not rows:
            break
        for entry_id, fields in rows:
            scanned += 1
            evidence = evidence_from_stream_row(entry_id, fields)
            if evidence is not None:
                terminals += 1
                run_id = evidence["run_id"]
                identity = (evidence["stream_entry_id"], evidence["event_id"], evidence["event_type"])
                previous = seen_terminal_entries.get(run_id)
                if previous is not None and previous != identity:
                    raise TerminalEventEvidenceError(
                        f"multiple historical terminal events exist for run {run_id!r}"
                    )
                seen_terminal_entries[run_id] = identity
                if await _upsert_historical_evidence(redis, evidence):
                    created += 1
        last_id = _text(rows[-1][0])
        if last_id == upper:
            break
        cursor = f"({last_id}"

    if scanned != snapshot.length:
        raise TerminalEventEvidenceError("bounded migration did not cover the complete stream snapshot")

    await _validate_existing_index_evidence(
        redis,
        seen_terminal_entries=seen_terminal_entries,
        page_size=page_size,
    )

    pipeline_factory = getattr(redis, "pipeline", None)
    if not callable(pipeline_factory):
        raise TerminalEventEvidenceError("Redis transaction support is required")
    try:
        from redis.exceptions import WatchError
    except Exception as exc:  # pragma: no cover
        raise TerminalEventEvidenceError("redis-py WatchError support is required") from exc

    marker = {
        READINESS_STATUS_FIELD: READY_STATUS,
        READINESS_RESULTS_STREAM_FIELD: stream,
        READINESS_SOURCE_HISTORY_FIELD: "1",
        READINESS_SNAPSHOT_LAST_ID_FIELD: upper,
        READINESS_ENTRIES_ADDED_FIELD: str(snapshot.entries_added),
        READINESS_SCANNED_FIELD: str(scanned),
        READINESS_TERMINAL_EVENTS_FIELD: str(terminals),
        READINESS_EVIDENCE_CREATED_FIELD: str(created),
        READINESS_COMPLETED_AT_FIELD: str(completed_at_ms),
    }
    async with pipeline_factory(transaction=True) as pipe:
        try:
            await pipe.watch(stream, index_key)
            finish_snapshot = stream_history_snapshot(await pipe.xinfo_stream(stream))
            if finish_snapshot != snapshot:
                raise TerminalEventEvidenceError("results stream changed during migration; rerun")
            contract_values = await pipe.hmget(index_key, *CONTRACT_FIELDS)
            validate_index_contract(
                {field: value for field, value in zip(CONTRACT_FIELDS, contract_values) if value is not None}
            )
            if int(await pipe.ttl(index_key)) != -1:
                raise TerminalEventEvidenceError("terminal-event index must be persistent")
            if await pipe.hget(index_key, READINESS_STATUS_FIELD) is not None:
                raise TerminalEventEvidenceError("migration readiness changed concurrently")
            pipe.multi()
            pipe.hset(index_key, mapping=marker)
            pipe.persist(index_key)
            await pipe.execute()
        except WatchError as exc:
            raise TerminalEventEvidenceError("results stream or evidence index changed during readiness commit; rerun") from exc

    return BackfillResult(
        status="ready",
        scanned_entries=scanned,
        terminal_events=terminals,
        evidence_created=created,
        snapshot_last_entry_id=upper,
    )
