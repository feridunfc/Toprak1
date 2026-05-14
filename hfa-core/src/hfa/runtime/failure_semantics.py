
from __future__ import annotations

from enum import Enum


class FailureCategory(Enum):
    TRANSIENT = "transient"
    TERMINAL = "terminal"
    UNKNOWN = "unknown"


class DistributedError(Exception):
    def __init__(self, message: str, category: FailureCategory, original_exc: Exception | None = None):
        super().__init__(message)
        self.category = category
        self.original_exc = original_exc


def classify_redis_error(exc: Exception) -> FailureCategory:
    exc_str = str(exc).lower()

    transient_markers = [
        "timeout",
        "timed out",
        "connection",
        "connection reset",
        "socket",
        "temporarily unavailable",
        "try again",
        "busy loading",
        "loading redis is loading",
        "readonly you can't write against a read only replica",
    ]
    terminal_markers = [
        "noscript",
        "wrongtype",
        "syntax",
        "unknown command",
        "invalid expire time",
        "value is not an integer",
    ]

    if any(marker in exc_str for marker in transient_markers):
        return FailureCategory.TRANSIENT

    if any(marker in exc_str for marker in terminal_markers):
        return FailureCategory.TERMINAL

    return FailureCategory.UNKNOWN
