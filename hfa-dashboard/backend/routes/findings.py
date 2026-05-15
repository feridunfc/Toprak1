from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from read_models.authority_audit import AuthorityAuditReader, AuthorityAuditReadError

router = APIRouter(prefix="/dashboard", tags=["findings"])


@router.get("/findings")
async def findings(
    limit: int = Query(25, ge=1, le=500),
    severity: str | None = Query(None, pattern="^(allowed|suspicious|banned)$"),
) -> dict[str, object]:
    try:
        items = AuthorityAuditReader.from_env().findings(limit=limit, severity=severity)
        return {"items": items, "count": len(items), "limit": limit, "severity": severity}
    except AuthorityAuditReadError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
