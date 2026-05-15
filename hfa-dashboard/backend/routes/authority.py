from __future__ import annotations

from fastapi import APIRouter, HTTPException
from read_models.authority_audit import AuthorityAuditReader, AuthorityAuditReadError

router = APIRouter(prefix="/dashboard/authority", tags=["authority"])


@router.get("")
async def authority_status() -> dict[str, object]:
    try:
        return AuthorityAuditReader.from_env().dashboard()
    except AuthorityAuditReadError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/heatmap")
async def authority_heatmap() -> list[dict[str, object]]:
    try:
        return AuthorityAuditReader.from_env().heatmap()
    except AuthorityAuditReadError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
