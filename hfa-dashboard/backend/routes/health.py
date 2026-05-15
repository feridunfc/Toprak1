from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/health")
async def health() -> dict[str, object]:
    return {"ok": True, "service": "hfa-dashboard", "mode": "read-only", "writes_enabled": False}
