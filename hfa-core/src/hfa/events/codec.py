"""
hfa-core/src/hfa/events/codec.py
IRONCLAD Sprint 10/11 — Event codec (serialise / deserialise)

Redis Streams store values as byte-strings.
serialize_event()  -> dict[str, str]  ready for XADD
decode_field()     -> typed Python     called by HFAEvent.from_redis()

IRONCLAD rules
--------------
* No print() — logging only.
* Never raise from decode_field() — return safe defaults on parse error.
* cost_cents always int — no float USD.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, is_dataclass
from typing import Any, Dict

logger = logging.getLogger(__name__)


def serialize_event(event: Any, *, omit_none_trace: bool = False) -> Dict[str, str]:
    if not is_dataclass(event):
        raise TypeError("serialize_event expects dataclass")

    data = asdict(event)
    out: Dict[str, str] = {}

    for k, v in data.items():
        if k.startswith("_"):
            continue

        if omit_none_trace and k in {"trace_parent", "trace_state"} and v is None:
            continue

        if v is None:
            out[k] = ""
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v, separators=(",", ":"))
        else:
            out[k] = str(v)

    return out


def decode_field(field_name: str, raw: Any, type_hint: str) -> Any:
    """
    Decode a single Redis Streams byte-value to the annotated Python type.
    On any parse error: returns a safe zero/empty default and logs a warning.
    """
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if raw == "" or raw is None:
        return _default_for(type_hint)

    json_hints = ("Dict[str, Any]", "List[int]", "List[str]", "dict", "list")
    if type_hint in json_hints or type_hint.startswith(("Dict", "List")):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(
                "codec.decode_field: JSON error field=%s raw=%r",
                field_name,
                raw[:80],
            )
            return _default_for(type_hint)

    if type_hint == "int":
        try:
            return int(raw)
        except (ValueError, TypeError):
            logger.warning(
                "codec.decode_field: int cast failed field=%s raw=%r",
                field_name,
                raw,
            )
            return 0

    if type_hint == "float":
        try:
            return float(raw)
        except (ValueError, TypeError):
            logger.warning(
                "codec.decode_field: float cast failed field=%s raw=%r",
                field_name,
                raw,
            )
            return 0.0

    if type_hint == "bool":
        return raw.lower() in ("true", "1", "yes")

    return raw


def _default_for(type_hint: str) -> Any:
    if type_hint == "int":
        return 0
    if type_hint == "float":
        return 0.0
    if type_hint == "bool":
        return False
    if type_hint.startswith("List"):
        return []
    if type_hint.startswith("Dict"):
        return {}
    if type_hint.startswith("Optional"):
        return None
    return ""


def safe_decode_str(data: Dict[bytes, bytes], key: str, default: str = "") -> str:
    raw = data.get(key.encode())
    if raw is None:
        raw = data.get(key)  # type: ignore[arg-type]
    if raw is None:
        return default
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw) if raw is not None else default


def safe_decode_int(data: Dict[bytes, bytes], key: str, default: int = 0) -> int:
    raw = data.get(key.encode())
    if raw is None:
        # Also check string key in case caller passes a str-keyed dict
        raw = data.get(key)  # type: ignore[arg-type]
    if raw is None:
        return default
    try:
        return int(raw.decode()) if isinstance(raw, bytes) else int(raw)
    except (ValueError, TypeError):
        return default


def safe_decode_float(
    data: Dict[bytes, bytes], key: str, default: float = 0.0
) -> float:
    raw = data.get(key.encode())
    if raw is None:
        raw = data.get(key)  # type: ignore[arg-type]
    if raw is None:
        return default
    try:
        return float(raw.decode()) if isinstance(raw, bytes) else float(raw)
    except (ValueError, TypeError):
        return default


def safe_decode_json(data: Dict[bytes, bytes], key: str, default: Any = None) -> Any:
    _sentinel = object()
    raw = data.get(key.encode(), _sentinel)
    if raw is _sentinel:
        raw = data.get(key, _sentinel)  # type: ignore[arg-type]
    if raw is _sentinel:
        return default if default is not None else {}
    try:
        if isinstance(raw, bytes):
            return json.loads(raw.decode())
        if isinstance(raw, str):
            return json.loads(raw)
        return default if default is not None else {}
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        return default if default is not None else {}


def deserialize_run_requested(data: Dict[bytes, bytes]) -> Any:
    from hfa.events.schema import RunRequestedEvent

    return RunRequestedEvent(
        run_id=safe_decode_str(data, "run_id"),
        tenant_id=safe_decode_str(data, "tenant_id"),
        agent_type=safe_decode_str(data, "agent_type"),
        priority=safe_decode_int(data, "priority", 5),
        payload=safe_decode_json(data, "payload", {}),
        idempotency_key=safe_decode_str(data, "idempotency_key"),
        trace_parent=safe_decode_str(data, "trace_parent") or None,
        trace_state=safe_decode_str(data, "trace_state") or None,
    )


def deserialize_run_completed(data: Dict[bytes, bytes]) -> Any:
    """
    Deserialise a RunCompletedEvent from a Redis Streams payload.
    Missing / unknown fields are tolerated — returns safe defaults.
    """
    from hfa.events.schema import RunCompletedEvent

    return RunCompletedEvent(
        run_id=safe_decode_str(data, "run_id"),
        tenant_id=safe_decode_str(data, "tenant_id"),
        worker_id=safe_decode_str(data, "worker_id"),
        cost_cents=safe_decode_int(data, "cost_cents", 0),
        tokens_used=safe_decode_int(data, "tokens_used", 0),
        payload=safe_decode_json(data, "payload", {}),
        trace_parent=safe_decode_str(data, "trace_parent") or None,
        trace_state=safe_decode_str(data, "trace_state") or None,
    )


def deserialize_run_failed(data: Dict[bytes, bytes]) -> Any:
    """
    Deserialise a RunFailedEvent from a Redis Streams payload.
    Missing / unknown fields are tolerated — returns safe defaults.
    """
    from hfa.events.schema import RunFailedEvent

    return RunFailedEvent(
        run_id=safe_decode_str(data, "run_id"),
        tenant_id=safe_decode_str(data, "tenant_id"),
        worker_id=safe_decode_str(data, "worker_id"),
        error=safe_decode_str(data, "error"),
        cost_cents=safe_decode_int(data, "cost_cents", 0),
        tokens_used=safe_decode_int(data, "tokens_used", 0),
        payload=safe_decode_json(data, "payload", {}),
        trace_parent=safe_decode_str(data, "trace_parent") or None,
        trace_state=safe_decode_str(data, "trace_state") or None,
    )
