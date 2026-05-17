from __future__ import annotations

from fastapi import APIRouter, Query

from read_models.events import EventStreamReadModel

router = APIRouter(prefix="/dashboard/events", tags=["events"])


@router.get("")
async def event_stream(limit: int = Query(25, ge=1, le=200)) -> dict[str, object]:
    return EventStreamReadModel.from_env().snapshot(limit=limit)
