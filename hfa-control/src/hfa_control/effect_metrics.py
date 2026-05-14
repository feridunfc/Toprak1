"""
hfa-control/src/hfa_control/effect_metrics.py
IRONCLAD Phase 7C — Lightweight suppress/fence counters.

Thread-safe in-process counters; production'da Prometheus exporter'a bağlanır.
Test isolation için reset_all() metodu mevcuttur.
"""
from __future__ import annotations

import threading
from collections import defaultdict

# ---------------------------------------------------------------------------
# Known counter names (sabit referanslar — string literal hatası önler)
# ---------------------------------------------------------------------------
DUPLICATE_DISPATCH_SUPPRESSED = "duplicate_dispatch_suppressed_total"
DUPLICATE_COMPLETION_SUPPRESSED = "duplicate_completion_suppressed_total"
STALE_COMPLETION_FENCED = "stale_completion_fenced_total"

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_counters: dict[str, int] = defaultdict(int)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def increment(name: str) -> None:
    """Atomically increment named counter by 1."""
    with _lock:
        _counters[name] += 1


def get(name: str) -> int:
    """Return current value of named counter (0 if never incremented)."""
    with _lock:
        return _counters[name]


def snapshot() -> dict[str, int]:
    """Return a copy of all counters (safe for serialisation/export)."""
    with _lock:
        return dict(_counters)


def reset_all() -> None:
    """
    Reset all counters to zero.
    ONLY for test isolation — do NOT call in production code.
    """
    with _lock:
        _counters.clear()
