from __future__ import annotations

from fastapi import APIRouter, Query

from read_models.workers import WorkerTelemetryReadModel

router = APIRouter(prefix="/dashboard/workers", tags=["workers"])


@router.get("")
async def worker_telemetry(limit: int = Query(50, ge=1, le=200)) -> dict[str, object]:
    return WorkerTelemetryReadModel.from_env().snapshot(limit=limit)
