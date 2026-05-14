"""
hfa-tools/src/hfa_tools/middleware/ledger.py
IRONCLAD — FastAPI middleware that auto-appends a SignedLedger entry
for every request/response cycle.

Design
------
* Fires AFTER the route handler returns (post-response hook).
* Captures: tenant_id, run_id, method, path, status_code,
  duration_ms, response_size_bytes.
* Never blocks the response — ledger write is fire-and-forget
  (errors are logged but do NOT propagate to the client).
* Requires TenantMiddleware to run first (reads request.state.tenant).
* Requires SignedLedger to be app.state.ledger (set at startup).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from hfa_tools.middleware.tenant import TenantContext

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EVENT_TYPE_REQUEST = "http_request"
_SKIP_PATHS_DEFAULT = frozenset({"/health", "/metrics", "/docs", "/openapi.json"})

# Max payload bytes to include in ledger (avoid huge blobs)
_MAX_RESPONSE_SNIPPET_BYTES = 256


# ---------------------------------------------------------------------------
# LedgerMiddleware
# ---------------------------------------------------------------------------


class LedgerMiddleware(BaseHTTPMiddleware):
    """
    Auto-appends a signed ledger entry for each HTTP request.

    The entry payload contains:
      - method, path, status_code
      - duration_ms (wall-clock)
      - response_size_bytes
      - tenant_id, run_id (from TenantContext injected by TenantMiddleware)
      - error (if any exception was raised in route handler)

    Prerequisites
    -------------
    1. ``app.state.ledger`` must be a ``SignedLedger`` instance.
    2. ``TenantMiddleware`` must be registered BEFORE ``LedgerMiddleware``
       so that ``request.state.tenant`` is populated.

    Usage
    -----
    app.add_middleware(TenantMiddleware)   # first
    app.add_middleware(LedgerMiddleware)   # second

    Args:
        app:        ASGI application.
        skip_paths: Set of path prefixes to skip ledger write.
        event_type: Override default event_type label in ledger.
    """

    def __init__(
        self,
        app: ASGIApp,
        skip_paths: Optional[frozenset[str]] = None,
        event_type: str = _EVENT_TYPE_REQUEST,
    ) -> None:
        super().__init__(app)
        self._skip = skip_paths if skip_paths is not None else _SKIP_PATHS_DEFAULT
        self._event_type = event_type
        logger.info(
            "LedgerMiddleware initialised: event_type=%s skip_paths=%s",
            event_type,
            self._skip,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Execute route handler, then fire-and-forget ledger write.

        Args:
            request:   Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response (unmodified).
        """
        # Skip health/metrics/docs endpoints
        for skip in self._skip:
            if request.url.path.startswith(skip):
                return await call_next(request)

        start_ns = time.perf_counter_ns()
        error_detail: Optional[str] = None
        status_code: int = 500

        try:
            response: Response = await call_next(request)
            status_code = response.status_code
        except Exception as exc:
            error_detail = f"{type(exc).__name__}: {exc}"
            logger.error("Route handler raised: %s path=%s", error_detail, request.url.path)
            # Re-raise so FastAPI exception handlers can process it
            raise
        finally:
            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000

            # Fire-and-forget: never block client response
            asyncio.get_running_loop().create_task(  # ✅ IRONCLAD: get_running_loop
                self._write_ledger_entry(
                    request=request,
                    status_code=status_code,
                    duration_ms=duration_ms,
                    error_detail=error_detail,
                )
            )

        return response

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    async def _write_ledger_entry(
        self,
        request: Request,
        status_code: int,
        duration_ms: float,
        error_detail: Optional[str],
    ) -> None:
        """
        Build and append the ledger entry.
        All exceptions are swallowed and logged — never propagate.
        """
        try:
            ledger = getattr(getattr(request, "app", None), "state", None)
            ledger = getattr(ledger, "ledger", None) if ledger else None

            if ledger is None:
                logger.warning("LedgerMiddleware: app.state.ledger not set — skipping ledger write")
                return

            # Extract tenant context (populated by TenantMiddleware)
            tenant_ctx: Optional[TenantContext] = getattr(request.state, "tenant", None)
            if tenant_ctx is None:
                logger.debug(
                    "LedgerMiddleware: no TenantContext on request.state — "
                    "skipping ledger write for path=%s",
                    request.url.path,
                )
                return

            payload = {
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": round(duration_ms, 3),
                "client_host": (request.client.host if request.client else None),
            }
            if error_detail:
                payload["error"] = error_detail[:_MAX_RESPONSE_SNIPPET_BYTES]

            run_id = tenant_ctx.run_id or f"unscoped-{tenant_ctx.tenant_id}"

            await ledger.append(
                tenant_id=tenant_ctx.tenant_id,
                run_id=run_id,
                event_type=self._event_type,
                payload=payload,
            )

        except Exception as exc:
            # Ledger failure must NEVER affect the HTTP response
            logger.error("LedgerMiddleware._write_ledger_entry failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Startup / shutdown helpers for app lifespan
# ---------------------------------------------------------------------------


async def setup_ledger(app, key_provider, store=None) -> None:
    """
    Convenience helper: create and attach a SignedLedger to app.state.

    Call this inside your FastAPI lifespan or startup event:

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await setup_ledger(app, key_provider=Ed25519EnvKeyProvider())
            yield
            await teardown_ledger(app)

    Args:
        app:          FastAPI application instance.
        key_provider: KeyProvider implementation (Ed25519EnvKeyProvider, etc.).
        store:        Optional LedgerStore; defaults to InMemoryLedgerStore.
    """
    from hfa.governance.signed_ledger_v1 import SignedLedger

    ledger = SignedLedger(key_provider=key_provider, store=store)
    app.state.ledger = ledger
    logger.info("SignedLedger attached to app.state: key_id=%s", key_provider.key_id)


async def teardown_ledger(app) -> None:
    """
    Detach the ledger from app.state during shutdown.
    Placeholder for future flush/close logic (e.g. RedisLedgerStore drain).

    Args:
        app: FastAPI application instance.
    """
    ledger = getattr(getattr(app, "state", None), "ledger", None)
    if ledger is not None:
        app.state.ledger = None
        logger.info("SignedLedger detached from app.state")
