"""
Authoritative event append gate for IRONCLAD / HFA.

This module is intentionally small.  It does not replace the existing
EventStore and it does not change replay semantics.  It provides a single
feature-flagged entry gate for authoritative write paths that must not mutate
runtime truth unless the corresponding event has been durably appended first.

Rollback:
    Set IRON_V3_EVENT_GATE to a false value to return to legacy behavior.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Optional, Protocol

logger = logging.getLogger(__name__)

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


class EventAppender(Protocol):
    async def append_event(
        self,
        *,
        run_id: str,
        event_type: str,
        worker_id: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> bool: ...


class AuthoritativeEventAppendError(RuntimeError):
    """Raised when an authoritative write cannot prove durable event append."""


@dataclass(frozen=True)
class EventAppendVerdict:
    ok: bool
    gated: bool
    event_type: str
    run_id: str
    reason: str = ""


def is_event_gate_enabled() -> bool:
    """Return True when authoritative write paths must append events first."""
    return os.getenv("IRON_V3_EVENT_GATE", "0") not in _FALSE_VALUES


class AuthoritativeEventGate:
    """
    Minimal feature-flagged guard for authoritative mutations.

    When disabled, callers may preserve legacy behavior.  When enabled, a
    missing EventStore, a false append result, or an append exception is treated
    as a fail-closed condition.  This keeps replay ahead of Redis/projection
    mutation for the guarded vertical slice.
    """

    def __init__(self, event_store: EventAppender | None, *, enabled: bool | None = None) -> None:
        self._event_store = event_store
        self._enabled = is_event_gate_enabled() if enabled is None else bool(enabled)

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def append_before_authoritative_write(
        self,
        *,
        run_id: str,
        event_type: str,
        worker_id: str | None = None,
        details: dict[str, Any] | None = None,
        authority: str,
    ) -> EventAppendVerdict:
        if not self._enabled:
            return EventAppendVerdict(
                ok=True,
                gated=False,
                event_type=event_type,
                run_id=run_id,
                reason="event_gate_disabled",
            )

        if self._event_store is None:
            logger.error(
                "Authoritative event gate blocked write: missing event_store run_id=%s event=%s authority=%s",
                run_id,
                event_type,
                authority,
            )
            raise AuthoritativeEventAppendError(
                f"event gate enabled but event_store is missing for {event_type}"
            )

        gated_details = dict(details or {})
        gated_details.setdefault("authority", authority)
        gated_details.setdefault("event_gate", "IRON_V3_EVENT_GATE")

        try:
            appended = await self._event_store.append_event(
                run_id=run_id,
                event_type=event_type,
                worker_id=worker_id,
                details=gated_details,
            )
        except Exception as exc:
            logger.error(
                "Authoritative event gate append raised: run_id=%s event=%s authority=%s error=%s",
                run_id,
                event_type,
                authority,
                exc,
                exc_info=True,
            )
            raise AuthoritativeEventAppendError(
                f"event append raised before authoritative write: {event_type}"
            ) from exc

        if not appended:
            logger.error(
                "Authoritative event gate blocked write: append returned false run_id=%s event=%s authority=%s",
                run_id,
                event_type,
                authority,
            )
            raise AuthoritativeEventAppendError(
                f"event append failed before authoritative write: {event_type}"
            )

        return EventAppendVerdict(
            ok=True,
            gated=True,
            event_type=event_type,
            run_id=run_id,
            reason="event_appended_before_write",
        )
