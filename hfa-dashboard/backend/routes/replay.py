from __future__ import annotations

from fastapi import APIRouter, HTTPException

from read_models.authority_audit import AuthorityAuditReadError
from read_models.replay import ReplayReadModel

router = APIRouter(prefix="/dashboard/replay", tags=["replay"])


@router.get("")
async def replay_snapshot() -> dict[str, object]:
    try:
        return ReplayReadModel.from_env().snapshot()
    except AuthorityAuditReadError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
