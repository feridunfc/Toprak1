"""
hfa-core/src/hfa/obs/otel.py
IRONCLAD Sprint 5 — OpenTelemetry integration

Responsibilities
----------------
* Span factory:  create_span() returns a no-op context manager when OTel is
  not configured — callers never need try/except around tracing.
* Metrics:       HFAMetrics records request count, latency (histogram), LLM
                 token usage, budget spend (integer cents), healing retries.
* Log bridge:    add_log_to_span() attaches structured log records to the
                 active span so logs and traces correlate in Jaeger/Grafana.
* Trace context: inject_trace_headers() / extract_trace_context() for
                 propagation across service boundaries (HTTP headers).

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop().
* All monetary values in INTEGER CENTS (no float USD in metrics).
* OTel SDK is OPTIONAL — if not installed, all helpers degrade gracefully
  to no-ops so that hfa-core does not require opentelemetry at import time
  for unit tests.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OTel SDK optional import — graceful degradation
# ---------------------------------------------------------------------------

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.metrics import get_meter_provider, set_meter_provider
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagators.composite import CompositePropagator

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    logger.info("opentelemetry not installed — OTel features disabled (no-op mode)")


# ---------------------------------------------------------------------------
# HFA Span attribute keys (constants)
# ---------------------------------------------------------------------------


class SpanAttr:
    """Standard attribute key constants for HFA spans."""

    TENANT_ID = "hfa.tenant_id"
    RUN_ID = "hfa.run_id"
    AGENT_TYPE = "hfa.agent_type"
    ATTEMPT = "hfa.healing.attempt"
    TOKENS_USED = "hfa.llm.tokens_used"
    COST_CENTS = "hfa.budget.cost_cents"  # ✅ integer cents
    SANDBOX_LANG = "hfa.sandbox.language"
    TEST_STATUS = "hfa.test.status"
    COMPLIANCE = "hfa.compliance.decision"
    ERROR_FP = "hfa.healing.error_fingerprint"
    GRAPH_NODE_ID = "hfa.graph.node_id"
    GRAPH_DEPTH = "hfa.graph.depth"


# ---------------------------------------------------------------------------
# OTel setup
# ---------------------------------------------------------------------------


class OTelConfig:
    """
    Bootstrap OpenTelemetry SDK from environment / explicit config.

    Usage
    -----
        cfg = OTelConfig(
            service_name="hfa-tools",
            otlp_endpoint="http://jaeger:4317",
        )
        cfg.setup()

    Environment Variables (override constructor args)
    -------------------------------------------------
        OTEL_SERVICE_NAME         default "hfa"
        OTEL_EXPORTER_OTLP_ENDPOINT  default "http://localhost:4317"
    """

    def __init__(
        self,
        service_name: str = "hfa",
        otlp_endpoint: Optional[str] = None,
        enabled: bool = True,
    ) -> None:
        import os

        self.service_name = os.getenv("OTEL_SERVICE_NAME", service_name)
        self.otlp_endpoint = otlp_endpoint or os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
        )
        self.enabled = enabled
        self._tracer: Optional[Any] = None
        self._meter: Optional[Any] = None

    def setup(self) -> None:
        """
        Initialise TracerProvider + MeterProvider.
        No-op if opentelemetry SDK is not installed or enabled=False.
        """
        if not self.enabled or not _OTEL_AVAILABLE:
            logger.info(
                "OTel setup skipped (enabled=%s, sdk=%s)", self.enabled, _OTEL_AVAILABLE
            )
            return

        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter,
            )
            from opentelemetry.sdk.resources import Resource

            resource = Resource.create({"service.name": self.service_name})

            # Tracer
            span_exporter = OTLPSpanExporter(endpoint=self.otlp_endpoint, insecure=True)
            tracer_provider = TracerProvider(resource=resource)
            tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
            trace.set_tracer_provider(tracer_provider)

            # Meter
            metric_exporter = OTLPMetricExporter(
                endpoint=self.otlp_endpoint, insecure=True
            )
            metric_reader = PeriodicExportingMetricReader(
                metric_exporter, export_interval_millis=15_000
            )
            meter_provider = MeterProvider(
                resource=resource, metric_readers=[metric_reader]
            )
            set_meter_provider(meter_provider)

            self._tracer = trace.get_tracer(self.service_name)
            self._meter = get_meter_provider().get_meter(self.service_name)
            logger.info(
                "OTel configured: service=%s endpoint=%s",
                self.service_name,
                self.otlp_endpoint,
            )
        except Exception as exc:
            logger.warning("OTel setup failed (non-fatal): %s", exc)

    def get_tracer(self):
        """Return active tracer or None if OTel not configured."""
        if _OTEL_AVAILABLE and self._tracer is None:
            self._tracer = trace.get_tracer(self.service_name)
        return self._tracer

    def get_meter(self):
        """Return active meter or None if OTel not configured."""
        if _OTEL_AVAILABLE and self._meter is None:
            self._meter = get_meter_provider().get_meter(self.service_name)
        return self._meter


# ---------------------------------------------------------------------------
# Span factory — graceful no-op
# ---------------------------------------------------------------------------

# Module-level default config (overridden by setup())
_default_config = OTelConfig(enabled=False)


def configure(cfg: OTelConfig) -> None:
    """Set the module-level OTel config. Call once at app startup."""
    global _default_config
    _default_config = cfg


@contextmanager
def create_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
    config: Optional[OTelConfig] = None,
) -> Generator:
    """
    Context manager that creates an OTel span if OTel is available,
    otherwise returns a no-op context manager.

    Usage
    -----
        with create_span("architect.plan", {SpanAttr.TENANT_ID: tenant_id}) as span:
            result = await architect.create_plan(request)
            if span:
                span.set_attribute(SpanAttr.TOKENS_USED, result.total_tokens)

    Args:
        name:       Span name (e.g. "hfa.architect.plan").
        attributes: Initial span attributes dict.
        config:     OTelConfig to use (defaults to module-level config).

    Yields:
        Active span object (set attributes on it), or None in no-op mode.
    """
    cfg = config or _default_config
    tracer = cfg.get_tracer() if cfg else None

    if tracer is None or not _OTEL_AVAILABLE:
        yield None
        return

    with tracer.start_as_current_span(name) as span:
        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, v)
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(trace.StatusCode.ERROR, str(exc))
            raise


def add_log_to_span(
    message: str,
    level: str = "INFO",
    attributes: Optional[Dict[str, Any]] = None,
    config: Optional[OTelConfig] = None,
) -> None:
    """
    Add a structured log event to the currently active OTel span.
    No-op if OTel is unavailable or no span is active.

    Args:
        message:    Log message string.
        level:      Log level string ("INFO", "WARN", "ERROR").
        attributes: Additional key-value attributes for the log event.
        config:     OTelConfig to use.
    """
    if not _OTEL_AVAILABLE:
        return
    cfg = config or _default_config
    if cfg is None:
        return
    try:
        current_span = trace.get_current_span()
        if current_span and current_span.is_recording():
            event_attrs = {"log.level": level, "log.message": message}
            if attributes:
                event_attrs.update(attributes)
            current_span.add_event("log", attributes=event_attrs)
    except Exception as exc:
        logger.debug("add_log_to_span failed (non-fatal): %s", exc)


def inject_trace_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """
    Inject W3C TraceContext + Baggage headers into `headers` dict.
    Returns the same dict (mutated) for chaining.
    No-op if OTel unavailable.

    Usage
    -----
        outbound_headers = inject_trace_headers({"Content-Type": "application/json"})
        await httpx_client.post(url, headers=outbound_headers)
    """
    if not _OTEL_AVAILABLE:
        return headers
    try:
        propagator = CompositePropagator(
            [
                TraceContextTextMapPropagator(),
                W3CBaggagePropagator(),
            ]
        )
        propagator.inject(headers)
    except Exception as exc:
        logger.debug("inject_trace_headers failed (non-fatal): %s", exc)
    return headers


def extract_trace_context(headers: Dict[str, str]) -> Any:
    """
    Extract W3C TraceContext from inbound `headers`.
    Returns OTel Context object (or None if unavailable).

    Usage
    -----
        ctx = extract_trace_context(dict(request.headers))
        with tracer.start_as_current_span("span", context=ctx):
            ...
    """
    if not _OTEL_AVAILABLE:
        return None
    try:
        propagator = CompositePropagator(
            [
                TraceContextTextMapPropagator(),
                W3CBaggagePropagator(),
            ]
        )
        return propagator.extract(headers)
    except Exception as exc:
        logger.debug("extract_trace_context failed (non-fatal): %s", exc)
        return None


# ---------------------------------------------------------------------------
# HFAMetrics — typed metric helpers
# ---------------------------------------------------------------------------


class HFAMetrics:
    """
    Typed metric facade for HFA.

    All monetary values are INTEGER CENTS — no float USD in metrics.
    Instantiate once per service and reuse across requests.

    Usage
    -----
        metrics = HFAMetrics(config=otel_cfg)
        metrics.record_request("architect", "acme_corp", latency_ms=120)
        metrics.record_llm_tokens("gpt-4o", tokens=512, cost_cents=8)
        metrics.record_healing_retry("acme_corp", "run-x", attempt=2)
    """

    def __init__(self, config: Optional[OTelConfig] = None) -> None:
        self._config = config or _default_config
        self._meter = None
        self._counters: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        self._initialized = False

    def _ensure_instruments(self) -> bool:
        """Lazily create metric instruments. Returns True if meter available."""
        if self._initialized:
            return self._meter is not None
        self._initialized = True
        meter = self._config.get_meter() if self._config else None
        if meter is None:
            return False
        self._meter = meter
        self._counters["requests"] = meter.create_counter(
            "hfa.requests.total", description="Total agent requests"
        )
        self._counters["errors"] = meter.create_counter(
            "hfa.errors.total", description="Total agent errors"
        )
        self._counters["tokens"] = meter.create_counter(
            "hfa.llm.tokens.total", description="LLM tokens used"
        )
        self._counters["cost_cents"] = meter.create_counter(
            "hfa.budget.cost_cents", description="Budget spent (integer cents)"
        )
        self._counters["healing_retries"] = meter.create_counter(
            "hfa.healing.retries", description="Healing engine retries"
        )
        self._counters["circuit_opens"] = meter.create_counter(
            "hfa.healing.circuit_opens", description="Circuit breaker openings"
        )
        self._histograms["latency_ms"] = meter.create_histogram(
            "hfa.request.duration_ms", unit="ms", description="Request latency"
        )
        self._histograms["sandbox_ms"] = meter.create_histogram(
            "hfa.sandbox.duration_ms",
            unit="ms",
            description="Sandbox execution latency",
        )
        return True

    def record_request(
        self,
        agent_type: str,
        tenant_id: str,
        latency_ms: float,
        success: bool = True,
    ) -> None:
        """Record a completed agent request."""
        if not self._ensure_instruments():
            return
        attrs = {
            "agent": agent_type,
            "tenant": tenant_id,
            "success": str(success).lower(),
        }
        self._counters["requests"].add(1, attrs)
        if not success:
            self._counters["errors"].add(1, attrs)
        self._histograms["latency_ms"].record(latency_ms, attrs)

    def record_llm_tokens(
        self,
        model: str,
        tokens: int,
        cost_cents: int,  # ✅ integer cents
        tenant_id: str = "unknown",
    ) -> None:
        """Record LLM token usage and cost."""
        if not self._ensure_instruments():
            return
        attrs = {"model": model, "tenant": tenant_id}
        self._counters["tokens"].add(tokens, attrs)
        self._counters["cost_cents"].add(cost_cents, attrs)

    def record_healing_retry(
        self,
        tenant_id: str,
        run_id: str,
        attempt: int,
    ) -> None:
        """Record a healing engine retry attempt."""
        if not self._ensure_instruments():
            return
        attrs = {"tenant": tenant_id, "attempt": str(attempt)}
        self._counters["healing_retries"].add(1, attrs)

    def record_circuit_open(self, tenant_id: str, run_id: str) -> None:
        """Record a circuit breaker opening."""
        if not self._ensure_instruments():
            return
        self._counters["circuit_opens"].add(1, {"tenant": tenant_id})

    def record_sandbox(
        self,
        language: str,
        duration_ms: float,
        success: bool,
    ) -> None:
        """Record sandbox execution metrics."""
        if not self._ensure_instruments():
            return
        attrs = {"language": language, "success": str(success).lower()}
        self._histograms["sandbox_ms"].record(duration_ms, attrs)
