"""
hfa-control/src/hfa_control/api/hitl_router.py
IRONCLAD OS — HITL (Human-In-The-Loop) API

PURPOSE
-------
When an agent workflow returns status="escalated" with requires_hitl=True,
the task stays RUNNING (heartbeat holds it). A human must approve or reject
via this endpoint before the agent retries or terminates.

ENDPOINTS
---------
GET  /v1/hitl/pending                   — list tasks awaiting human decision
GET  /v1/hitl/{run_id}                  — get HITL request detail
POST /v1/hitl/{run_id}/approve          — approve: agent DAG resumes
POST /v1/hitl/{run_id}/reject           — reject: task fails permanently
POST /v1/hitl/{run_id}/modify           — approve with modified context

STORAGE
-------
HITL state lives in hfa-semantic Redis (not IRONCLAD state machine):
    semantic:hitl:{run_id}  →  {status, reason, context, requested_at_ms}
TTL: 48 hours (after which HITL request expires → task auto-fails)

IRONCLAD INTEGRATION
--------------------
On approve: CognitiveExecutor retries with modified context.
On reject:  StateStore marks task FAILED (existing IRONCLAD path).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger("hfa.hitl")

router = APIRouter(prefix="/v1/hitl", tags=["HITL"])

_HITL_TTL_SECONDS = 48 * 3600   # 48-hour HITL window


# ── Request/Response models ──────────────────────────────────────────────────

class HITLRequest(BaseModel):
    run_id: str
    tenant_id: str
    agent_type: str
    reason: str                          # why HITL was triggered
    reasoning_trace: List[str] = Field(default_factory=list)
    suggested_feedback: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)   # agent artifacts
    requested_at_ms: int = Field(default_factory=lambda: int(time.time() * 1000))


class HITLDecision(BaseModel):
    approved: bool
    comment: Optional[str] = None
    modified_context: Optional[Dict[str, Any]] = None   # override agent context
    decided_by: Optional[str] = None


class HITLResponse(BaseModel):
    run_id: str
    status: str   # "pending" | "approved" | "rejected" | "expired"
    decision: Optional[HITLDecision] = None
    request: Optional[HITLRequest] = None


# ── Redis helpers ────────────────────────────────────────────────────────────

def _hitl_key(run_id: str) -> str:
    return f"semantic:hitl:{run_id}"


async def _redis(request: Request):
    """Get Redis from app state (set at startup)."""
    redis = getattr(request.app.state, "semantic_redis", None)
    if redis is None:
        redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(503, "Redis not available")
    return redis


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/{run_id}/request", response_model=HITLResponse)
async def request_hitl(
    run_id: str,
    body: HITLRequest,
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
) -> HITLResponse:
    """
    Called by CognitiveExecutor when an agent returns requires_hitl=True.
    Stores the HITL request; a human must then approve or reject.
    """
    redis = await _redis(request)
    key = _hitl_key(run_id)

    record = {
        "run_id":           run_id,
        "status":           "pending",
        "request":          body.model_dump(),
        "decision":         None,
        "created_at_ms":    int(time.time() * 1000),
    }
    await redis.set(key, json.dumps(record), ex=_HITL_TTL_SECONDS)
    logger.info("HITL requested run=%s tenant=%s reason=%s", run_id, x_tenant_id, body.reason)

    return HITLResponse(run_id=run_id, status="pending", request=body)


@router.get("/pending", response_model=List[HITLResponse])
async def list_pending(
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
) -> List[HITLResponse]:
    """
    List all HITL requests pending for a tenant.
    Uses Redis SCAN — bounded by key count, not full keyspace scan.
    """
    redis = await _redis(request)
    results = []
    async for key in redis.scan_iter("semantic:hitl:*", count=100):
        raw = await redis.get(key)
        if not raw:
            continue
        try:
            record = json.loads(raw)
            req_data = record.get("request", {})
            if req_data.get("tenant_id") != x_tenant_id:
                continue
            if record["status"] != "pending":
                continue
            results.append(HITLResponse(
                run_id=record["run_id"],
                status=record["status"],
                request=HITLRequest(**req_data) if req_data else None,
            ))
        except Exception as exc:
            logger.debug("HITL list parse error key=%s: %s", key, exc)

    return results


@router.get("/{run_id}", response_model=HITLResponse)
async def get_hitl(
    run_id: str,
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
) -> HITLResponse:
    redis = await _redis(request)
    raw = await redis.get(_hitl_key(run_id))
    if not raw:
        raise HTTPException(404, f"HITL request not found for run_id={run_id}")
    record = json.loads(raw)
    req_data = record.get("request", {})
    if req_data.get("tenant_id") != x_tenant_id:
        raise HTTPException(403, "Tenant mismatch")
    return HITLResponse(
        run_id=run_id,
        status=record["status"],
        request=HITLRequest(**req_data) if req_data else None,
        decision=HITLDecision(**record["decision"]) if record.get("decision") else None,
    )


@router.post("/{run_id}/approve", response_model=HITLResponse)
async def approve_hitl(
    run_id: str,
    body: HITLDecision,
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
) -> HITLResponse:
    """
    Approve a HITL request. The CognitiveExecutor polls this and resumes
    the agent DAG with optional modified_context.
    """
    return await _decide(run_id, True, body, request, x_tenant_id)


@router.post("/{run_id}/reject", response_model=HITLResponse)
async def reject_hitl(
    run_id: str,
    body: HITLDecision,
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
) -> HITLResponse:
    """
    Reject a HITL request. CognitiveExecutor returns ExecutionResult(failed).
    IRONCLAD marks the task permanently failed.
    """
    return await _decide(run_id, False, body, request, x_tenant_id)


async def _decide(
    run_id: str,
    approved: bool,
    body: HITLDecision,
    request: Request,
    tenant_id: str,
) -> HITLResponse:
    redis = await _redis(request)
    key = _hitl_key(run_id)
    raw = await redis.get(key)
    if not raw:
        raise HTTPException(404, f"HITL request not found for run_id={run_id}")

    record = json.loads(raw)
    req_data = record.get("request", {})
    if req_data.get("tenant_id") != tenant_id:
        raise HTTPException(403, "Tenant mismatch")
    if record["status"] != "pending":
        raise HTTPException(409, f"HITL already decided: {record['status']}")

    body.approved = approved   # ensure it matches the endpoint
    record["status"] = "approved" if approved else "rejected"
    record["decision"] = body.model_dump()
    record["decided_at_ms"] = int(time.time() * 1000)

    await redis.set(key, json.dumps(record), ex=_HITL_TTL_SECONDS)
    logger.info("HITL decided run=%s approved=%s by=%s", run_id, approved, body.decided_by)

    return HITLResponse(
        run_id=run_id,
        status=record["status"],
        decision=body,
    )
