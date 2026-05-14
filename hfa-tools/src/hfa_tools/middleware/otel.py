"""
hfa-tools/src/hfa_tools/middleware/otel.py
IRONCLAD Sprint 5 — OTel tracing + metrics middleware

Wraps every HTTP request in a span and records latency/status metrics.
Reads W3C TraceContext from inbound headers for distributed tracing.
Extracts HFA-specific attributes (tenant, run_id) from request.state.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop().
* Middleware errors NEVER propagate to client — always degrade gracefully.
* Span is created even if OTel not configured (no-op span from hfa.obs.otel).
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional, Set

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from hfa.obs.otel import (
    OTelConfig,
    SpanAttr,
    create_span,
    add_log_to_span,
    extract_trace_context,
    HFAMetrics,
    _default_config,
)

logger = logging.getLogger(__name__)


class OTelMiddleware(BaseHTTPMiddleware):
    """
    FastAPI/Starlette middleware that instruments every HTTP request with:
      * An OTel span (span name: "hfa.http <METHOD> <path>").
      * HFAMetrics.record_request() on completion.
      * W3C TraceContext extraction from inbound headers.
      * HFA attributes: tenant_id, run_id attached to span if present.

    Skip paths (e.g. /health, /metrics) bypass tracing entirely.

    Usage
    -----
        app.add_middleware(
            OTelMiddleware,
            config=otel_cfg,
            skip_paths={"/health", "/metrics"},
        )

    Args:
        app:        ASGI application.
        config:     OTelConfig (defaults to module-level config).
        skip_paths: Set of path prefixes to skip.
    """

    def __init__(
        self,
        app: ASGIApp,
        config: Optional[OTelConfig] = None,
        skip_paths: Optional[Set[str]] = None,
    ) -> None:
        super().__init__(app)
        self._config = config or _default_config
        self._skip_paths = skip_paths or {"/health", "/metrics", "/docs", "/openapi.json"}
        self._metrics = HFAMetrics(config=self._config)
        logger.info("OTelMiddleware initialised: skip_paths=%s", self._skip_paths)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip telemetry for health/metrics endpoints
        for skip in self._skip_paths:
            if request.url.path.startswith(skip):
                return await call_next(request)

        span_name = f"hfa.http {request.method} {request.url.path}"
        start_ts = time.time()

        # Extract tenant/run_id from request state (set by TenantMiddleware)
        tenant_id = "unknown"
        run_id = None
        try:
            ctx = getattr(request.state, "tenant", None)
            if ctx:
                tenant_id = ctx.tenant_id
                run_id = ctx.run_id
        except Exception:
            pass

        # Extract trace context from inbound headers
        upstream_ctx = None
        try:
            upstream_ctx = extract_trace_context(dict(request.headers))
        except Exception:
            pass

        # Initial span attributes
        attrs = {
            SpanAttr.TENANT_ID: tenant_id,
            "http.method": request.method,
            "http.url": str(request.url),
            "http.target": request.url.path,
        }
        if run_id:
            attrs[SpanAttr.RUN_ID] = run_id

        success = True
        status_code = 500
        response: Optional[Response] = None

        try:
            with create_span(span_name, attributes=attrs, config=self._config) as span:
                try:
                    response = await call_next(request)
                    status_code = response.status_code
                    success = status_code < 500

                    if span:
                        span.set_attribute("http.status_code", status_code)
                        add_log_to_span(
                            f"{request.method} {request.url.path} → {status_code}",
                            level="INFO" if success else "ERROR",
                            config=self._config,
                        )

                except Exception:
                    success = False
                    if span:
                        span.set_attribute("http.status_code", 500)
                    raise

        except Exception:
            # Telemetry errors MUST NOT propagate — return 500 response
            if response is None:
                response = Response(content="Internal server error", status_code=500)

        finally:
            duration_ms = (time.time() - start_ts) * 1000
            try:
                # Extract agent type from path for metrics label
                path_parts = request.url.path.strip("/").split("/")
                agent_type = path_parts[-1] if path_parts else "unknown"
                self._metrics.record_request(
                    agent_type=agent_type,
                    tenant_id=tenant_id,
                    latency_ms=duration_ms,
                    success=success,
                )
            except Exception as exc:
                logger.debug("OTelMiddleware metrics error (non-fatal): %s", exc)

        return response or Response(content="Internal server error", status_code=500)
