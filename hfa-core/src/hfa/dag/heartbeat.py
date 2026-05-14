
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HeartbeatPolicy:
    stale_after_ms: int = 30_000
    heartbeat_interval_ms: int = 5_000
    max_requeue_count: int = 3
