
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from hfa_control.event_store import EventStore

logger = logging.getLogger(__name__)


def emit_event_background(
    event_store: Optional[EventStore],
    *,
    run_id: str,
    event_type: str,
    worker_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    if event_store is None:
        return

    async def _emit() -> None:
        await event_store.append_event(
            run_id=run_id,
            event_type=event_type,
            worker_id=worker_id,
            details=details,
        )

    try:
        asyncio.create_task(_emit())
    except RuntimeError:
        logger.warning("Event hook skipped: no running loop (run_id=%s event=%s)", run_id, event_type)
