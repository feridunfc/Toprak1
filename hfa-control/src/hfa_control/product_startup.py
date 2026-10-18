"""Read-only SINGLE_TASK startup checks; never provision or repair a domain."""
from __future__ import annotations

import os
import re
import select
import stat
import time

from hfa.config.keys import RedisKey
from hfa_control.run_terminal_event_evidence import (
    CONTRACT_FIELDS, READINESS_FIELDS, TerminalEventEvidenceError,
    _decode_evidence, _mapping, _text, evidence_from_stream_row,
    stream_history_snapshot, validate_readiness,
)


def redis_client_from_environment(url: str, *, decode_responses: bool = False):
    """Optional inherited password pipe is authentication, never ownership proof."""
    import redis.asyncio as redis

    options = {}
    raw_fd = os.environ.pop("HFA_REDIS_PASSWORD_FD", None)
    if raw_fd is not None:
        fd = None
        try:
            fd = int(raw_fd)
            if fd < 3 or not stat.S_ISFIFO(os.fstat(fd).st_mode):
                raise ValueError("private pipe required")
            deadline, secret = time.monotonic() + 2, b""
            while len(secret) <= 256:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
                    raise ValueError("credential channel timeout")
                chunk = os.read(fd, 257 - len(secret))
                if not chunk:
                    break
                secret += chunk
            password = secret.removesuffix(b"\n")
            if not password or len(secret) > 256 or b"\n" in password:
                raise ValueError("invalid credential length")
            options["password"] = password.decode("utf-8", errors="strict")
        except (OSError, ValueError):
            raise RuntimeError("invalid private Redis password descriptor") from None
        finally:
            if fd is not None and fd >= 3:
                try:
                    os.close(fd)
                except OSError:
                    pass
    return redis.from_url(url, decode_responses=decode_responses, **options)


async def verify_terminal_event_readiness(redis, *, page_size: int = 500) -> dict[str, str]:
    """Validate retained producer/readiness/history evidence using reads only.

    A concurrent terminal append causes a bounded retry. This is an admission
    prerequisite, not an authority decision: existing Lua guards still validate
    evidence atomically at effect time. No migration runs on service startup.
    """
    if type(page_size) is not int or not 1 <= page_size <= 5000:
        raise ValueError("invalid startup page size")
    index, stream = RedisKey.run_terminal_event_index(), RedisKey.stream_results()
    try:
        for _attempt in range(3):
            if _text(await redis.type(index)) != "hash" or int(await redis.pttl(index)) != -1:
                raise TerminalEventEvidenceError("terminal-event index missing, wrong-type or expiring")
            if _text(await redis.type(stream)) != "stream" or int(await redis.pttl(stream)) != -1:
                raise TerminalEventEvidenceError("results stream missing, wrong-type or expiring")
            fields = CONTRACT_FIELDS + READINESS_FIELDS
            marker = dict(zip(fields, await redis.hmget(index, *fields)))
            ready = validate_readiness(marker)
            for name in READINESS_FIELDS[4:]:
                if not re.fullmatch(r"0|[1-9][0-9]*", ready.get(name, "")):
                    raise TerminalEventEvidenceError("terminal-event readiness counters incomplete or corrupt")
            saved_id = ready[READINESS_FIELDS[3]]
            if not re.fullmatch(r"[0-9]+-[0-9]+", saved_id):
                raise TerminalEventEvidenceError("terminal-event snapshot identity corrupt")
            initial = stream_history_snapshot(await redis.xinfo_stream(stream))
            added, scanned, terminals, created = [int(ready[name]) for name in READINESS_FIELDS[4:8]]
            if (added != scanned or created > terminals or terminals > scanned or added > initial.entries_added
                    or tuple(map(int, saved_id.split("-"))) > tuple(map(int, initial.last_generated_id.split("-")))):
                raise TerminalEventEvidenceError("terminal-event readiness contradicts results history")
            seen, cursor, count, historical_count, historical_terminals = {}, "-", 0, 0, 0
            while True:
                rows = await redis.xrange(stream, min=cursor, max=initial.last_generated_id, count=page_size)
                if not rows:
                    break
                for entry_id, row in rows:
                    count += 1
                    event = evidence_from_stream_row(entry_id, row)
                    if tuple(map(int, _text(entry_id).split("-"))) <= tuple(map(int, saved_id.split("-"))):
                        historical_count += 1
                        historical_terminals += int(event is not None)
                    if event is not None:
                        run_id = event["run_id"]
                        if run_id in seen:
                            raise TerminalEventEvidenceError("multiple terminal events for one RUN")
                        seen[run_id] = event
                        stored = _decode_evidence(await redis.hget(index, RedisKey.run_terminal_event_evidence_field(run_id)))
                        if any(stored.get(name) != event[name] for name in (
                                "run_id", "tenant_id", "event_id", "event_type", "final_state", "stream_entry_id")):
                            raise TerminalEventEvidenceError("terminal-event evidence contradicts results stream")
                        row = _mapping(row)
                        for name in ("canonical_transition_id", "canonical_record_hash", "canonical_revision", "terminal_proof_sha256"):
                            if name in row and stored.get(name) != row[name]:
                                raise TerminalEventEvidenceError("canonical terminal-event evidence contradicts stream")
                        if stored["source"] == "canonical_run_terminate":
                            from hfa.authority import AggregateType, CanonicalAggregateIdentity, RedisCanonicalAuthorityStore
                            probe = await RedisCanonicalAuthorityStore(redis).load_receipt_probe(
                                CanonicalAggregateIdentity(AggregateType.RUN, run_id), stored.get("operation_id", ""))
                            record = None if probe is None else probe.canonical_store_record
                            if (record is None or record.operation_type != "RUN_TERMINATE"
                                    or stored.get("canonical_transition_id") != record.transition_id
                                    or stored.get("canonical_record_hash") != record.canonical_record_hash
                                    or stored.get("canonical_command_hash") != record.canonical_command_hash
                                    or stored.get("canonical_revision") != str(record.to_revision)
                                    or stored.get("terminal_proof_sha256") != record.authoritative_metadata_changes["terminal_evidence"]["terminal_proof_sha256"]):
                                raise TerminalEventEvidenceError("terminal-event evidence contradicts canonical RUN proof")
                cursor = "(" + _text(rows[-1][0])
            cursor, indexed = 0, set()
            while True:
                cursor, rows = await redis.hscan(index, cursor=cursor, match="run:*", count=page_size)
                for field, raw in rows.items():
                    proof = _decode_evidence(raw)
                    if (_text(field) != RedisKey.run_terminal_event_evidence_field(proof["run_id"])
                            or proof["run_id"] not in seen):
                        raise TerminalEventEvidenceError("terminal-event index has orphan or contradictory evidence")
                    indexed.add(proof["run_id"])
                if int(cursor) == 0:
                    break
            if (initial == stream_history_snapshot(await redis.xinfo_stream(stream))
                    and marker == dict(zip(fields, await redis.hmget(index, *fields)))
                    and int(await redis.pttl(index)) == -1 and int(await redis.pttl(stream)) == -1
                    and count == initial.length and indexed == set(seen)):
                if historical_count != scanned or historical_terminals != terminals:
                    raise TerminalEventEvidenceError("readiness snapshot counters contradict source history")
                return ready
        raise TerminalEventEvidenceError("terminal-event history changed during startup validation")
    except TerminalEventEvidenceError as exc:
        raise RuntimeError(f"SINGLE_TASK_ALPHA startup blocked: {exc}") from None
    except Exception as exc:
        raise RuntimeError(f"SINGLE_TASK_ALPHA startup readiness unavailable ({type(exc).__name__})") from None
