from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from hfa_control.event_store import EventStore

logger = logging.getLogger(__name__)


async def emit_event_checked(
    event_store: Optional[EventStore],
    *,
    run_id: str,
    event_type: str,
    worker_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> bool:
    """
    Append an event on the caller's path and return the append result.

    This helper preserves the existing EventStore contract while making the
    authority boundary explicit for callers that cannot rely on fire-and-forget
    emission.  It does not raise on EventStore false returns, but exceptions are
    allowed to propagate so strict callers can fail closed.
    """
    if event_store is None:
        logger.warning(
            "Checked event hook skipped: missing event_store (run_id=%s event=%s)",
            run_id,
            event_type,
        )
        return False
    return await event_store.append_event(
        run_id=run_id,
        event_type=event_type,
        worker_id=worker_id,
        details=details,
    )


def emit_event_background(
    event_store: Optional[EventStore],
    *,
    run_id: str,
    event_type: str,
    worker_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """
    Legacy non-authoritative event hook.

    This remains fire-and-forget for compatibility.  It must not be used as the
    proof of truth for terminal or otherwise authoritative mutations, because a
    background append can be skipped or fail after Redis/projection state has
    already changed.
    """
    if event_store is None:
        return

    async def _emit() -> None:
        ok = await event_store.append_event(
            run_id=run_id,
            event_type=event_type,
            worker_id=worker_id,
            details=details,
        )
        if not ok:
            logger.error(
                "Background event append failed after caller continued: run_id=%s event=%s",
                run_id,
                event_type,
            )

    try:
        asyncio.create_task(_emit())
    except RuntimeError:
        logger.warning("Event hook skipped: no running loop (run_id=%s event=%s)", run_id, event_type)
