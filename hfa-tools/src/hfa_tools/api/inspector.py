"""
hfa-tools/src/hfa_tools/api/inspector.py
IRONCLAD Sprint 7 â€” Run Inspector API (Sprint 6 â†’ Sprint 7 upgrade)

Sprint 6 baseline changes
--------------------------
Sprint 6 RunRegistry was memory-only: restart = data loss, no archive.
Sprint 6 SSE had no heartbeat, no archived-run fallback, no disconnect guard.
Sprint 6 summary had no created_at, so tenant lists sorted by run_id (wrong).

Sprint 7 additions
------------------
RunRegistry (Faz 1 â€” Persistence)
  âœ… Hybrid storage: live runs in _graphs (RAM), archived in Redis (JSON)
  âœ… get_live_graph(run_id)       â†’ RAM only; None if archived
  âœ… get_snapshot(run_id)         â†’ RAM first, Redis fallback; None if absent
  âœ… get_tenant_run_summaries()   â†’ active (RAM) + historical (Redis) merged
  âœ… _normalize_summary()         â†’ uniform shape regardless of source
  âœ… created_at in every summary  â†’ correct time-based sort, not run_id lex
  âœ… _archive_run() uses redis.pipeline(transaction=True)  â€” atomic SET+ZADD
  âœ… history_ttl_seconds param    â†’ every archived key gets a TTL
  âœ… archive failure â†’ run stays in RAM, never silently evicted
  âœ… Redis down â†’ graceful degrade to RAM-only, no crash

SSE (Faz 3 â€” Resilience)
  âœ… Heartbeat ": heartbeat" every 15 s â†’ proxy timeout guard
  âœ… is_disconnected() per loop  â†’ no zombie generator on client drop
  âœ… CancelledError re-raised    â†’ clean server shutdown
  âœ… Archived run â†’ single "event: complete" then close immediately
  âœ… Tenant mismatch â†’ 403 before generator opens (fail-fast)
  âœ… X-Accel-Buffering: no header

Redis key schema
----------------
  hfa:run:{run_id}              STRING   JSON snapshot, TTL = history_ttl_seconds
  hfa:tenant_runs:{tenant_id}   ZSET     member = run_id, score = created_at float

Redis unavailable policy
------------------------
  get_snapshot():               log error â†’ return None â†’ caller raises 404
  get_tenant_run_summaries():   log error â†’ return RAM-only list
  _archive_run():               returns False â†’ cleanup skips eviction

Mini Paket 2 hardening (Persistence Hardening)
-----------------------------------------------
  âœ… _archive_run() returns bool (True=success, False=failure) â€” no raise
  âœ… _cleanup_loop evicts ONLY when archive returns True
  âœ… get_snapshot(): JSON decode error handled separately from Redis error
  âœ… _normalize_summary(): created_at fallback = 0.0 when source is missing it
  âœ… SUMMARY_REQUIRED_FIELDS constant documents the guaranteed shape
  âœ… Expired/TTL-expired history â†’ 404 (same as never-existed; no tombstone)
  âœ… Duplicate archive attempt is harmless (ZADD overwrites score idempotently)
  âœ… All config params documented; no magic numbers

IRONCLAD rules
--------------
  * No print() â€” logging only.
  * No asyncio.get_event_loop() â€” get_running_loop() where needed.
  * close() always safe to call multiple times.
  * 404 / 403 / 400 / 503 â€” typed, never 500 from missing data.
  * cost_cents: int â€” no float USD.

Sprint 8 Mini 3 (Tracing)
--------------------------
  âœ… hfa.inspector.snapshot_lookup span: get_run_graph, get_run_summary,
     list_tenant_runs â€” with try/except so 404/403 paths are also traced
  âœ… hfa.inspector.stream_open span: SSE endpoint stream-open decision
     (short span, ends before generator opens â€” no infinite span)
  âœ… 403/404 HTTPExceptions recorded on span via record_exc before re-raise
  âœ… hfa.archived attribute: "true"/"false" in both lookup and stream spans
  âœ… Tracing never blocks or breaks business logic (no-op safe)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from hfa.obs.run_graph import ExecutionGraph, NodeStatus
from hfa.obs.tracing import get_tracer, HFATracing  # Sprint 8 Mini 3

logger = logging.getLogger(__name__)
_tracer = get_tracer("hfa.inspector")

router = APIRouter(prefix="/v1/inspector", tags=["inspector"])


# ---------------------------------------------------------------------------
# Response models
# (Sprint 7 additions: GraphSummaryResponse.created_at, RunListItem.created_at)
# ---------------------------------------------------------------------------


class NodeResponse(BaseModel):
    node_id: str
    agent_type: str
    status: str
    depth: int
    parent_ids: List[str]
    started_at: Optional[float]
    finished_at: Optional[float]
    duration_ms: Optional[float]
    tokens_used: int
    cost_cents: int  # integer cents â€” no float USD
    error: Optional[str]
    input_hash: Optional[str]
    output_hash: Optional[str]
    metadata: Dict[str, Any]


class GraphSummaryResponse(BaseModel):
    run_id: str
    tenant_id: str
    created_at: float  # âœ… Sprint 7: timestamp for correct sort order
    total_nodes: int
    done: int
    failed: int
    running: int
    pending: int
    total_tokens: int
    total_cost_cents: int
    is_complete: bool
    has_failures: bool


class GraphResponse(BaseModel):
    run_id: str
    tenant_id: str
    nodes: List[NodeResponse]
    edges: Dict[str, List[str]]
    summary: GraphSummaryResponse


class RunListItem(BaseModel):
    run_id: str
    created_at: float  # âœ… Sprint 7: for time-ordered tenant list
    is_complete: bool
    has_failures: bool
    total_nodes: int
    running: int


# ---------------------------------------------------------------------------
# Summary contract
# ---------------------------------------------------------------------------

# Every summary dict â€” regardless of source (live ExecutionGraph or Redis-archived
# JSON blob) â€” must contain at least these fields after _normalize_summary().
SUMMARY_REQUIRED_FIELDS: frozenset = frozenset(
    {
        "run_id",
        "tenant_id",
        "created_at",
        "total_nodes",
        "done",
        "failed",
        "running",
        "pending",
        "total_tokens",
        "total_cost_cents",
        "is_complete",
        "has_failures",
    }
)


def _normalize_summary(
    raw: Dict[str, Any],
    run_id: str,
    tenant_id: str,
    created_at: Optional[float],
) -> Dict[str, Any]:
    """
    Guarantee a consistent summary shape from any source.

    Both live ExecutionGraph snapshots and Redis-archived JSON blobs pass
    through here so callers never need to handle missing fields.

    created_at:
        If None, falls back to 0.0 (sorts to bottom of tenant list â€” safe,
        not silent). A warning is logged so ops can detect this case.
    run_id / tenant_id:
        Always injected from caller â€” never trusted from ``raw``.

    The resulting dict is guaranteed to satisfy SUMMARY_REQUIRED_FIELDS.
    """
    resolved_ts = created_at if created_at is not None else 0.0
    if created_at is None:
        logger.warning(
            "_normalize_summary: created_at missing for run=%s, defaulting to 0.0",
            run_id,
        )
    return {
        "run_id": run_id,
        "tenant_id": tenant_id,
        "created_at": resolved_ts,
        "total_nodes": raw.get("total_nodes", 0),
        "done": raw.get("done", 0),
        "failed": raw.get("failed", 0),
        "running": raw.get("running", 0),
        "pending": raw.get("pending", 0),
        "total_tokens": raw.get("total_tokens", 0),
        "total_cost_cents": raw.get("total_cost_cents", 0),
        "is_complete": raw.get("is_complete", False),
        "has_failures": raw.get("has_failures", False),
    }


# ---------------------------------------------------------------------------
# RunRegistry
# ---------------------------------------------------------------------------


class RunRegistry:
    """
    Hybrid run registry: live runs in RAM, completed runs archived in Redis.

    Public API (contracts tested in test_sprint7_inspector.py)
    -----------------------------------------------------------
      register(graph)                     â†’ add live graph (called from orchestrator)
      get_live_graph(run_id)              â†’ RAM only; None if archived or unknown
      get_snapshot(run_id)               â†’ RAM first, Redis fallback; None if absent
      get_tenant_run_summaries(tenant)   â†’ active + historical, newest first
      list_by_tenant(tenant)             â†’ sync RAM-only list (Sprint 6 compat)

    Internal layout
    ---------------
      _graphs:   Dict[run_id, ExecutionGraph]   â€” live runs
      _added_at: Dict[run_id, float]            â€” registration timestamp
      _redis:    Optional async Redis client
      _history_ttl: int                         â€” TTL for archived Redis keys

    Args
    ----
      graphs              Pre-populated dict from RunOrchestrator (optional).
      max_age             Seconds before a complete graph is archive-eligible.
      cleanup_interval    How often the background cleanup loop runs.
      redis_client        Async Redis client; None = memory-only mode.
      history_ttl_seconds TTL for archived Redis keys. Default: 7 days.
    """

    _KEY_PREFIX = "hfa:run:"
    _IDX_PREFIX = "hfa:tenant_runs:"

    def __init__(
        self,
        graphs: Optional[Dict[str, ExecutionGraph]] = None,
        max_age: float = 3600.0,
        cleanup_interval: float = 300.0,
        redis_client: Optional[Any] = None,
        history_ttl_seconds: int = 604_800,  # 7 days
    ) -> None:
        self._graphs: Dict[str, ExecutionGraph] = graphs if graphs is not None else {}
        self._max_age = max_age
        self._interval = cleanup_interval
        self._redis = redis_client
        self._history_ttl = history_ttl_seconds
        self._added_at: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        logger.info(
            "RunRegistry init: redis=%s max_age=%.0fs history_ttl=%ds",
            "enabled" if redis_client else "disabled",
            max_age,
            history_ttl_seconds,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start background archive/cleanup loop."""
        loop = asyncio.get_running_loop()
        self._cleanup_task = loop.create_task(
            self._cleanup_loop(), name="inspector.registry.cleanup"
        )
        logger.info("RunRegistry started")

    async def close(self) -> None:
        """Cancel cleanup loop. Safe to call multiple times."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def register(self, graph: ExecutionGraph) -> None:
        """Register a live ExecutionGraph (called by RunOrchestrator)."""
        async with self._lock:
            self._graphs[graph.run_id] = graph
            self._added_at[graph.run_id] = time.time()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_live_graph(self, run_id: str) -> Optional[ExecutionGraph]:
        """
        Return in-memory ExecutionGraph for SSE streaming.
        Never queries Redis â€” returns None for archived or unknown runs.
        """
        return self._graphs.get(run_id)

    # Sprint 6 compatibility alias
    def get(self, run_id: str) -> Optional[ExecutionGraph]:
        return self.get_live_graph(run_id)

    async def get_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Hybrid lookup: RAM first, Redis fallback.

        Returns a normalised snapshot dict, or None if absent everywhere.

        Error handling:
          Redis network error  â†’ log + return None (â†’ 404, not 500)
          JSON decode error    â†’ log + return None (corrupt entry treated as absent)
        """
        # 1. RAM hit â€” always wins
        g = self._graphs.get(run_id)
        if g is not None:
            created_at = self._added_at.get(run_id, time.time())
            snap = g.snapshot()
            snap["summary"] = _normalize_summary(snap["summary"], run_id, g.tenant_id, created_at)
            return snap

        # 2. Redis fallback
        if self._redis is not None:
            raw = None
            try:
                raw = await self._redis.get(f"{self._KEY_PREFIX}{run_id}")
            except Exception as exc:
                logger.error("RunRegistry.get_snapshot redis error run=%s: %s", run_id, exc)
                return None  # Redis unavailable â†’ treat as not-found

            if raw is not None:
                try:
                    text = raw.decode() if isinstance(raw, bytes) else raw
                    return json.loads(text)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    logger.error(
                        "RunRegistry.get_snapshot JSON decode error run=%s: %s",
                        run_id,
                        exc,
                    )
                    return None  # Corrupt entry â†’ treat as not-found

        return None

    async def get_tenant_run_summaries(
        self,
        tenant_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Merge active (RAM) + historical (Redis) summaries for a tenant.

        Rules:
          * Sort by created_at descending (newest first).
          * Active (RAM) wins on duplicate run_id.
          * Redis error â†’ log + return RAM-only (graceful degrade).
        """
        # Active (RAM) â€” authoritative source
        active: Dict[str, Dict] = {}
        for g in self._graphs.values():
            if g.tenant_id == tenant_id:
                ca = self._added_at.get(g.run_id, time.time())
                active[g.run_id] = _normalize_summary(
                    g.snapshot()["summary"], g.run_id, tenant_id, ca
                )

        # Historical (Redis) â€” only for runs not currently in RAM
        historical: Dict[str, Dict] = {}
        if self._redis is not None:
            try:
                rids = await self._redis.zrevrange(f"{self._IDX_PREFIX}{tenant_id}", 0, limit - 1)
                if rids:
                    keys = [
                        f"{self._KEY_PREFIX}{r.decode() if isinstance(r, bytes) else r}"
                        for r in rids
                    ]
                    raws = await self._redis.mget(keys)
                    for raw in raws:
                        if raw is None:
                            continue
                        snap = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                        rid = snap.get("run_id") or snap.get("summary", {}).get("run_id")
                        if rid and rid not in active:
                            historical[rid] = snap.get("summary", snap)
            except Exception as exc:
                logger.error(
                    "RunRegistry.get_tenant_run_summaries redis error tenant=%s: %s",
                    tenant_id,
                    exc,
                )
                # Degrade: return RAM-only below

        merged = {**historical, **active}  # active wins over historical
        return sorted(
            list(merged.values()),
            key=lambda s: s.get("created_at", 0.0),
            reverse=True,
        )[:limit]

    # Sprint 6 compat: sync RAM-only list
    def list_by_tenant(self, tenant_id: str, limit: int = 50) -> List[ExecutionGraph]:
        graphs = [g for g in self._graphs.values() if g.tenant_id == tenant_id]
        return sorted(graphs, key=lambda g: g.run_id, reverse=True)[:limit]

    # ------------------------------------------------------------------
    # Archive + cleanup
    # ------------------------------------------------------------------

    async def _archive_run(self, graph: ExecutionGraph, created_at: float) -> bool:
        """
        Persist snapshot to Redis atomically.

        Uses pipeline(transaction=True): SET (snapshot) and ZADD (tenant index)
        either both commit or neither does.

        Returns True on success, False on failure.
        Does NOT raise â€” caller checks return value and decides whether to evict.

        Idempotency:
            Calling twice for the same run_id is safe:
            * SET overwrites the previous snapshot (last-write-wins).
            * ZADD overwrites the score for the same member (no duplicates in ZSET).
            This means duplicate archive attempts are harmless.

        Expired entries (TTL elapsed):
            Redis evicts the snapshot key automatically. The ZSET member
            remains until a future ZADD or explicit cleanup. Callers will get
            None from get_snapshot() which the endpoint surfaces as 404.
        """
        if self._redis is None:
            return True  # memory-only mode: nothing to do, not a failure

        snap = graph.snapshot()
        snap["summary"] = _normalize_summary(
            snap["summary"], graph.run_id, graph.tenant_id, created_at
        )

        key = f"{self._KEY_PREFIX}{graph.run_id}"
        idx_key = f"{self._IDX_PREFIX}{graph.tenant_id}"
        payload = json.dumps(snap)

        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.set(key, payload, ex=self._history_ttl)  # TTL on every key
                pipe.zadd(idx_key, {graph.run_id: created_at})  # score = timestamp
                await pipe.execute()
            logger.info(
                "RunRegistry archived: run=%s tenant=%s ttl=%ds",
                graph.run_id,
                graph.tenant_id,
                self._history_ttl,
            )
            return True
        except Exception as exc:
            logger.error(
                "RunRegistry._archive_run FAILED run=%s (stays in RAM until next cycle): %s",
                graph.run_id,
                exc,
            )
            return False

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._interval)
                now = time.time()
                stale = []
                async with self._lock:
                    for rid, added in list(self._added_at.items()):
                        g = self._graphs.get(rid)
                        if g and g.is_complete() and now - added > self._max_age:
                            stale.append((g, added))

                evicted = 0
                for g, added in stale:
                    archived = await self._archive_run(g, added)  # bool, never raises
                    if archived:
                        async with self._lock:
                            self._graphs.pop(g.run_id, None)
                            self._added_at.pop(g.run_id, None)
                        evicted += 1
                    else:
                        logger.warning(
                            "RunRegistry: skipping eviction for run=%s (archive failed)",
                            g.run_id,
                        )

                if evicted:
                    logger.info("RunRegistry: archived+evicted %d graphs", evicted)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("RunRegistry cleanup error: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def _get_registry(request: Request) -> RunRegistry:
    registry = getattr(request.app.state, "run_registry", None)
    if registry is None:
        raise HTTPException(503, "Run registry not initialised")
    return registry


async def _require_snapshot(
    registry: RunRegistry,
    run_id: str,
    tenant_id: str,
) -> Dict[str, Any]:
    """
    Async equivalent of Sprint 6 _require_graph; supports archived runs.
    404 if absent, 403 if tenant mismatch.
    """
    snap = await registry.get_snapshot(run_id)
    if snap is None:
        raise HTTPException(404, f"Run {run_id!r} not found")
    if snap.get("tenant_id") != tenant_id:
        raise HTTPException(403, f"Run {run_id!r} does not belong to your tenant")
    return snap


def _require_tenant(x_tenant_id: Optional[str]) -> str:
    if not x_tenant_id:
        raise HTTPException(400, "X-Tenant-ID header required")
    return x_tenant_id


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------


def _node_resp(node) -> NodeResponse:
    """Build NodeResponse from GraphNode object."""
    return NodeResponse(
        node_id=node.node_id,
        agent_type=node.agent_type,
        status=node.status.value,
        depth=node.depth,
        parent_ids=node.parent_ids,
        started_at=node.started_at,
        finished_at=node.finished_at,
        duration_ms=node.duration_ms,
        tokens_used=node.tokens_used,
        cost_cents=node.cost_cents,
        error=node.error,
        input_hash=node.input_hash,
        output_hash=node.output_hash,
        metadata=node.metadata,
    )


def _graph_resp_from_snap(snap: Dict[str, Any]) -> GraphResponse:
    """Build GraphResponse from a normalised snapshot dict (live or archived)."""
    nodes = []
    for nd in snap.get("nodes", []):
        try:
            nodes.append(NodeResponse(**nd))
        except Exception as exc:
            logger.warning("NodeResponse parse error (skipped): %s", exc)
    return GraphResponse(
        run_id=snap["run_id"],
        tenant_id=snap["tenant_id"],
        nodes=nodes,
        edges={k: list(v) for k, v in snap.get("edges", {}).items()},
        summary=GraphSummaryResponse(**snap["summary"]),
    )


# ---------------------------------------------------------------------------
# Endpoints  (Sprint 7: async _require_snapshot; Sprint 8 Mini 3: tracing)
# ---------------------------------------------------------------------------


@router.get("/runs/{run_id}", response_model=GraphResponse)
async def get_run_graph(
    run_id: str,
    request: Request,
    x_tenant_id: Optional[str] = Header(None),
) -> GraphResponse:
    """Full graph snapshot â€” live or archived. 404/403/400 as appropriate."""
    tenant_id = _require_tenant(x_tenant_id)
    registry = _get_registry(request)

    with _tracer.start_as_current_span("hfa.inspector.snapshot_lookup") as span:
        HFATracing.set_attrs(
            span,
            {
                "hfa.run_id": run_id,
                "hfa.tenant_id": tenant_id,
                "hfa.endpoint": "get_run_graph",
            },
        )
        try:
            snap = await _require_snapshot(registry, run_id, tenant_id)
            HFATracing.set_attrs(
                span,
                {
                    "hfa.archived": str(registry.get_live_graph(run_id) is None).lower(),
                },
            )
            HFATracing.span_ok(span)
            return _graph_resp_from_snap(snap)
        except Exception as exc:
            HFATracing.record_exc(span, exc)
            raise


@router.get("/runs/{run_id}/nodes", response_model=List[NodeResponse])
async def get_run_nodes(
    run_id: str,
    request: Request,
    status: Optional[str] = Query(None),
    x_tenant_id: Optional[str] = Header(None),
) -> List[NodeResponse]:
    """Flat node list, optionally filtered by status."""
    tenant_id = _require_tenant(x_tenant_id)
    registry = _get_registry(request)
    snap = await _require_snapshot(registry, run_id, tenant_id)

    nodes = []
    for nd in snap.get("nodes", []):
        try:
            nodes.append(NodeResponse(**nd))
        except Exception:
            pass

    if status:
        try:
            NodeStatus(status)
            nodes = [n for n in nodes if n.status == status]
        except ValueError:
            raise HTTPException(400, f"Invalid status: {status!r}")
    return nodes


@router.get("/runs/{run_id}/summary", response_model=GraphSummaryResponse)
async def get_run_summary(
    run_id: str,
    request: Request,
    x_tenant_id: Optional[str] = Header(None),
) -> GraphSummaryResponse:
    """Lightweight summary â€” designed for fast polling (<5 ms)."""
    tenant_id = _require_tenant(x_tenant_id)
    registry = _get_registry(request)

    with _tracer.start_as_current_span("hfa.inspector.snapshot_lookup") as span:
        HFATracing.set_attrs(
            span,
            {
                "hfa.run_id": run_id,
                "hfa.tenant_id": tenant_id,
                "hfa.endpoint": "get_run_summary",
            },
        )
        try:
            snap = await _require_snapshot(registry, run_id, tenant_id)
            HFATracing.span_ok(span)
            return GraphSummaryResponse(**snap["summary"])
        except Exception as exc:
            HFATracing.record_exc(span, exc)
            raise


@router.get("/tenants/{tenant_id}/runs", response_model=List[RunListItem])
async def list_tenant_runs(
    tenant_id: str,
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    x_tenant_id: Optional[str] = Header(None),
) -> List[RunListItem]:
    """Active + historical runs for a tenant, newest first."""
    requesting = _require_tenant(x_tenant_id)
    if requesting != tenant_id:
        raise HTTPException(403, "Cannot access another tenant's runs")

    registry = _get_registry(request)

    with _tracer.start_as_current_span("hfa.inspector.snapshot_lookup") as span:
        HFATracing.set_attrs(
            span,
            {
                "hfa.tenant_id": tenant_id,
                "hfa.endpoint": "list_tenant_runs",
            },
        )
        try:
            summaries = await registry.get_tenant_run_summaries(tenant_id, limit=limit)
            HFATracing.set_attrs(span, {"hfa.result_count": len(summaries)})
            HFATracing.span_ok(span)
        except Exception as exc:
            HFATracing.record_exc(span, exc)
            raise

    return [
        RunListItem(
            run_id=s["run_id"],
            created_at=s.get("created_at", 0.0),
            is_complete=s.get("is_complete", False),
            has_failures=s.get("has_failures", False),
            total_nodes=s.get("total_nodes", 0),
            running=s.get("running", 0),
        )
        for s in summaries
    ]


@router.get("/runs/{run_id}/events")
async def stream_run_events(
    run_id: str,
    request: Request,
    x_tenant_id: Optional[str] = Header(None),
    interval_ms: int = Query(500, ge=100, le=5000),
) -> StreamingResponse:
    tenant_id = _require_tenant(x_tenant_id)
    registry = _get_registry(request)
    live_graph = registry.get_live_graph(run_id)

    archived_snapshot: Optional[Dict[str, Any]] = None

    with _tracer.start_as_current_span("hfa.inspector.stream_open") as span:
        HFATracing.set_attrs(
            span,
            {
                "hfa.run_id": run_id,
                "hfa.tenant_id": tenant_id,
            },
        )

        try:
            # Fail-fast tenant / archived lookup before StreamingResponse starts
            if live_graph is not None:
                HFATracing.set_attrs(span, {"hfa.archived": "false"})

                if live_graph.tenant_id != tenant_id:
                    raise HTTPException(403, "Tenant mismatch")
            else:
                archived_snapshot = await _require_snapshot(registry, run_id, tenant_id)
                HFATracing.set_attrs(span, {"hfa.archived": "true"})

            HFATracing.span_ok(span)

        except Exception as exc:
            HFATracing.record_exc(span, exc)
            raise

    async def _generate() -> AsyncGenerator[str, None]:
        # Archived path: single snapshot event then close
        if archived_snapshot is not None:
            yield (
                f"event: complete\n"
                f"data: {json.dumps(archived_snapshot['summary'], separators=(',', ':'))}\n\n"
            )
            return

        prev_json: Optional[str] = None
        heartbeat_interval = 15.0
        last_yield_ts = time.time()

        try:
            while True:
                if await request.is_disconnected():
                    logger.info("SSE client disconnected: run=%s", run_id)
                    break

                snap = live_graph.snapshot()
                summary = snap["summary"]
                summary_json = json.dumps(summary, separators=(",", ":"))
                now = time.time()

                if summary_json != prev_json:
                    prev_json = summary_json
                    yield f"data: {summary_json}\n\n"
                    last_yield_ts = now
                elif now - last_yield_ts > heartbeat_interval:
                    yield ": heartbeat\n\n"
                    last_yield_ts = now

                if summary["is_complete"]:
                    yield (f"event: complete\ndata: {summary_json}\n\n")
                    break

                await asyncio.sleep(interval_ms / 1000)

        except asyncio.CancelledError:
            logger.info("SSE cancelled (server shutdown): run=%s", run_id)
            raise

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )
