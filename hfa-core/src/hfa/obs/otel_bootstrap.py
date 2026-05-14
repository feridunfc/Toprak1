"""
hfa-core/src/hfa/obs/otel_bootstrap.py
IRONCLAD Sprint 8 Mini3 — OTel bootstrap (metrics + tracing)

Design goals
------------
- Safe bootstrap for metrics and tracing
- No crash if OTel SDK / exporters are missing
- Prometheus ASGI app created exactly once
- /metrics mount uses providers.prometheus_app only
- Env is read at bootstrap time, not import time
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class OTelProviders:
    meter_provider: Optional[Any] = None
    tracer_provider: Optional[Any] = None
    prometheus_app: Optional[Any] = None


def _setup_prometheus_metrics(
    result: OTelProviders,
    service_name: str,
) -> None:
    """
    Configure Prometheus metrics exporter and meter provider.

    Side effects:
      - sets metrics global meter provider
      - stores Prometheus ASGI app in result.prometheus_app

    Never raises.
    """
    try:
        from prometheus_client import make_asgi_app
        from opentelemetry import metrics
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.exporter.prometheus import PrometheusMetricReader
    except Exception as exc:
        logger.warning(
            "Prometheus metrics bootstrap unavailable (%s) -- /metrics disabled",
            exc,
        )
        return

    try:
        resource = Resource.create({"service.name": service_name})
        reader = PrometheusMetricReader()
        provider = MeterProvider(resource=resource, metric_readers=[reader])

        metrics.set_meter_provider(provider)

        result.meter_provider = provider
        result.prometheus_app = make_asgi_app()

        logger.info("OTel metrics bootstrap: Prometheus exporter enabled")
    except Exception as exc:
        logger.warning("Prometheus metrics bootstrap failed: %s", exc)


def _setup_otlp_metrics(
    result: OTelProviders,
    service_name: str,
    endpoint: str,
) -> None:
    """
    Configure OTLP metrics exporter.

    Never raises.
    """
    try:
        from opentelemetry import metrics
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )
    except Exception as exc:
        logger.warning(
            "OTLP metrics bootstrap unavailable (%s) -- metrics exporter disabled",
            exc,
        )
        return

    try:
        resource = Resource.create({"service.name": service_name})
        exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(exporter)
        provider = MeterProvider(resource=resource, metric_readers=[reader])

        metrics.set_meter_provider(provider)
        result.meter_provider = provider

        logger.info(
            "OTel metrics bootstrap: OTLP exporter enabled endpoint=%s", endpoint
        )
    except Exception as exc:
        logger.warning("OTLP metrics bootstrap failed: %s", exc)


def _setup_otlp_tracing(
    result: OTelProviders,
    service_name: str,
    endpoint: str,
) -> None:
    """
    Configure OTLP tracing exporter.

    Never raises.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
    except Exception as exc:
        logger.warning(
            "OTLP tracing bootstrap unavailable (%s) -- tracing exporter disabled",
            exc,
        )
        return

    try:
        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))

        trace.set_tracer_provider(provider)
        result.tracer_provider = provider

        logger.info(
            "OTel tracing bootstrap: OTLP exporter enabled endpoint=%s", endpoint
        )
    except Exception as exc:
        logger.warning("OTLP tracing bootstrap failed: %s", exc)


def _setup_console_tracing(
    result: OTelProviders,
    service_name: str,
) -> None:
    """
    Configure console/stdout tracing exporter for local debugging.

    Never raises.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (
            BatchSpanProcessor,
            ConsoleSpanExporter,
        )
    except Exception as exc:
        logger.warning(
            "Console tracing bootstrap unavailable (%s) -- tracing exporter disabled",
            exc,
        )
        return

    try:
        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

        trace.set_tracer_provider(provider)
        result.tracer_provider = provider

        logger.info("OTel tracing bootstrap: console exporter enabled")
    except Exception as exc:
        logger.warning("Console tracing bootstrap failed: %s", exc)


def bootstrap_otel() -> OTelProviders:
    """
    Bootstrap OTel metrics and tracing providers.

    Env vars
    --------
    OTEL_SERVICE_NAME                    default: hfa-platform
    OTEL_METRICS_EXPORTER                default: prometheus
      supported: prometheus | otlp | none
    OTEL_TRACES_EXPORTER                 default: none
      supported: otlp | console | none
    OTEL_EXPORTER_OTLP_ENDPOINT          default: http://localhost:4317

    Returns
    -------
    OTelProviders
    """
    service_name = os.getenv("OTEL_SERVICE_NAME", "hfa-platform")
    metrics_exp = os.getenv("OTEL_METRICS_EXPORTER", "prometheus").lower()
    traces_exp = os.getenv("OTEL_TRACES_EXPORTER", "none").lower()
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    result = OTelProviders()

    logger.info(
        "OTel bootstrap start: service=%s metrics=%s traces=%s",
        service_name,
        metrics_exp,
        traces_exp,
    )

    # Metrics
    if metrics_exp == "prometheus":
        _setup_prometheus_metrics(result, service_name)
    elif metrics_exp == "otlp":
        _setup_otlp_metrics(result, service_name, otlp_endpoint)
    elif metrics_exp == "none":
        logger.info("OTel metrics bootstrap: disabled")
    else:
        logger.warning(
            "Unknown OTEL_METRICS_EXPORTER=%r -- metrics disabled", metrics_exp
        )

    # Tracing
    if traces_exp == "otlp":
        _setup_otlp_tracing(result, service_name, otlp_endpoint)
    elif traces_exp == "console":
        _setup_console_tracing(result, service_name)
    elif traces_exp == "none":
        logger.info("OTel tracing bootstrap: disabled")
    else:
        logger.warning(
            "Unknown OTEL_TRACES_EXPORTER=%r -- tracing disabled", traces_exp
        )

    return result


def mount_prometheus(
    app: Any, providers: OTelProviders, path: str = "/metrics"
) -> None:
    """
    Mount Prometheus ASGI app if available.

    IMPORTANT:
      - Does NOT call make_asgi_app() again
      - Mounts only providers.prometheus_app
    """
    if providers.prometheus_app is None:
        logger.warning("Prometheus app not available -- %s not mounted", path)
        return

    try:
        app.mount(path, providers.prometheus_app)
        logger.info("Prometheus metrics mounted at %s", path)
    except Exception as exc:
        logger.warning("Failed to mount Prometheus app at %s: %s", path, exc)


async def shutdown_otel(providers: OTelProviders) -> None:
    """
    Shutdown OTel providers safely.

    Never raises.
    """
    if providers.tracer_provider is not None:
        try:
            providers.tracer_provider.shutdown()
            logger.info("OTel tracer provider shut down")
        except Exception as exc:
            logger.warning("OTel tracer provider shutdown failed: %s", exc)

    if providers.meter_provider is not None:
        try:
            providers.meter_provider.shutdown()
            logger.info("OTel meter provider shut down")
        except Exception as exc:
            logger.warning("OTel meter provider shutdown failed: %s", exc)
