
from __future__ import annotations

from threading import Lock

_payload_inline_count = 0
_payload_ref_count = 0
_payload_bytes_total = 0
_payload_store_errors_total = 0
_lock = Lock()


def reset_all() -> None:
    global _payload_inline_count, _payload_ref_count, _payload_bytes_total, _payload_store_errors_total
    with _lock:
        _payload_inline_count = 0
        _payload_ref_count = 0
        _payload_bytes_total = 0
        _payload_store_errors_total = 0


def inc_inline(count: int = 1) -> None:
    global _payload_inline_count
    with _lock:
        _payload_inline_count += count


def inc_ref(count: int = 1) -> None:
    global _payload_ref_count
    with _lock:
        _payload_ref_count += count


def add_bytes(size: int) -> None:
    global _payload_bytes_total
    with _lock:
        _payload_bytes_total += int(size)


def inc_errors(count: int = 1) -> None:
    global _payload_store_errors_total
    with _lock:
        _payload_store_errors_total += count


def snapshot() -> dict[str, int]:
    with _lock:
        return {
            "payload_inline_count": _payload_inline_count,
            "payload_ref_count": _payload_ref_count,
            "payload_bytes_total": _payload_bytes_total,
            "payload_store_errors_total": _payload_store_errors_total,
        }
