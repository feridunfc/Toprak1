"""
hfa-core/src/hfa/obs/tracing.py
IRONCLAD Sprint 8 Mini 3 — Tracing Helper

Why this file exists
--------------------
Instead of scattering raw OTel tracer calls across every module, this helper
provides a single stable surface. If the OTel SDK is absent, all operations
degrade silently to no-ops — the business logic is never affected.

Public API
----------
    get_tracer(name)                → opentelemetry.trace.Tracer (or no-op)
    HFATracing.set_attrs(span, d)   → set span attributes from a dict
    HFATracing.record_exc(span, e)  → record exception + set ERROR status
    HFATracing.span_ok(span)        → set OK status

Span naming convention
----------------------
    hfa.orchestrator.run           — root span per run in RunOrchestrator
    hfa.sandbox.execute            — per-execution span in DistributedSandboxPool
    hfa.inspector.snapshot_lookup  — per REST request in inspector endpoints
    hfa.inspector.stream_open      — SSE stream-open event in inspector

Attribute naming convention
---------------------------
    hfa.run_id         — safe as trace attribute (high cardinality allowed)
    hfa.tenant_id      — low cardinality; always included
    hfa.agent_type     — enum-like; low cardinality
    hfa.worker_id      — integer string; low cardinality
    hfa.node_id        — sandbox node; low cardinality in most clusters
    hfa.language       — runtime language; low cardinality
    hfa.status         — run status string; low cardinality
    hfa.archived       — "true"/"false"; low cardinality

High-cardinality guard
----------------------
run_id IS safe in trace attributes (one value per trace, that's the point).
run_id must NEVER appear as a metric label — that would explode cardinality.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop().
* OTel SDK absent → no crash, no log spam, silent no-op.
* Never raise from any HFATracing method.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tracer factory
# ---------------------------------------------------------------------------


def get_tracer(name: str = "hfa.core") -> Any:
    """
    Return an OTel Tracer for the given instrumentation scope.

    Falls back to a no-op tracer when the SDK is not installed or when
    no TracerProvider has been configured. This means production code
    can call start_as_current_span() unconditionally.
    """
    try:
        from opentelemetry import trace

        return trace.get_tracer(name)
    except ImportError:
        return _NoOpTracer()
    except Exception as exc:
        logger.debug("get_tracer fallback to no-op: %s", exc)
        return _NoOpTracer()


# ---------------------------------------------------------------------------
# HFATracing static helper
# ---------------------------------------------------------------------------


class HFATracing:
    """
    Static helpers for recording span attributes and errors.

    All methods are safe to call with a None span or a no-op span.
    They never raise.
    """

    @staticmethod
    def set_attrs(span: Any, attrs: Dict[str, Any]) -> None:
        """
        Set multiple span attributes from a dict.

        Silently skips None span and ignores OTel errors.
        Coerces values to str/int/float/bool as appropriate.
        """
        if span is None:
            return
        try:
            for k, v in attrs.items():
                if v is not None:
                    span.set_attribute(
                        k, str(v) if not isinstance(v, (bool, int, float)) else v
                    )
        except Exception as exc:
            logger.debug("HFATracing.set_attrs error: %s", exc)

    @staticmethod
    def record_exc(span: Any, exc: BaseException) -> None:
        """
        Record an exception on the span and set ERROR status.

        Uses Status(StatusCode.ERROR) wrapper for OTel Python API compatibility.
        Silently handles None span and no-op spans.
        """
        if span is None:
            return
        try:
            from opentelemetry.trace import Status, StatusCode

            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
        except ImportError:
            try:
                span.record_exception(exc)
            except Exception:
                pass
        except Exception as err:
            logger.debug("HFATracing.record_exc error: %s", err)

    @staticmethod
    def span_ok(span: Any) -> None:
        """Set span status to OK using Status wrapper."""
        if span is None:
            return
        try:
            from opentelemetry.trace import Status, StatusCode

            span.set_status(Status(StatusCode.OK))
        except ImportError:
            pass
        except Exception as exc:
            logger.debug("HFATracing.span_ok error: %s", exc)


# ---------------------------------------------------------------------------
# Convenience: safe span context manager
# ---------------------------------------------------------------------------


@contextmanager
def hfa_span(
    tracer: Any,
    name: str,
    attrs: Optional[Dict[str, Any]] = None,
) -> Generator[Any, None, None]:
    """
    Context manager that opens a span, sets attributes, and records any
    exception automatically. Never raises due to tracing failures.

    Usage::

        with hfa_span(tracer, "hfa.sandbox.execute", {"hfa.run_id": run_id}) as span:
            result = await do_work()

    On exception:
        * Exception is recorded on span with ERROR status.
        * Exception is re-raised normally (tracing never suppresses errors).
    """
    try:
        span_ctx = tracer.start_as_current_span(name)
    except Exception:
        # Tracer broken — yield None and continue
        yield None
        return

    try:
        with span_ctx as span:
            if attrs:
                HFATracing.set_attrs(span, attrs)
            try:
                yield span
            except Exception as exc:
                HFATracing.record_exc(span, exc)
                raise
    except Exception:
        # Any OTel framing error — re-raise original if span context broke
        raise


# ---------------------------------------------------------------------------
# No-op implementations (used when OTel SDK is absent)
# ---------------------------------------------------------------------------


class _NoOpSpan:
    """Minimal no-op span — absorbs all calls silently."""

    def set_attribute(self, key: Any, value: Any) -> None:  # noqa: D401
        pass

    def record_exception(self, exc: Any, **_: Any) -> None:
        pass

    def set_status(self, *_: Any, **__: Any) -> None:
        pass

    def __enter__(self) -> "_NoOpSpan":
        return self

    def __exit__(self, *_: Any) -> bool:
        return False  # do not suppress exceptions


class _NoOpTracer:
    """Minimal no-op tracer — start_as_current_span yields a _NoOpSpan."""

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        **_kwargs: Any,
    ) -> Generator[_NoOpSpan, None, None]:
        yield _NoOpSpan()
