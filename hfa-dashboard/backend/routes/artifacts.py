from __future__ import annotations

from fastapi import APIRouter, Query

from read_models.artifacts import ArtifactVaultReadModel

router = APIRouter(prefix="/dashboard/artifacts", tags=["artifacts"])


@router.get("")
async def artifacts_snapshot(limit: int = Query(25, ge=1, le=200)) -> dict[str, object]:
    return ArtifactVaultReadModel.from_env().snapshot(limit=limit)
