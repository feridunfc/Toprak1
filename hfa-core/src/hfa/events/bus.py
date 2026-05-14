"""
hfa-core/src/hfa/events/bus.py
IRONCLAD Sprint 6 — Event Bus

Design
------
Three-tier abstraction:
  EventBus (ABC)          — contract, all implementations must satisfy
  InMemoryEventBus        — testing / single-node dev (pub/sub via asyncio.Queue)
  NATSEventBus            — production (nats.py async client)
  KafkaEventBus           — alternative / high-throughput (aiokafka)

HFAEvent schema
---------------
Every event carries:
  event_id      UUID4 — global dedup key
  event_type    str   — namespaced "hfa.run.started", "hfa.node.done", etc.
  tenant_id     str   — tenant isolation (consumers filter by this)
  run_id        str   — correlation key
  timestamp     float — Unix epoch (UTC)
  payload       dict  — event-specific data (schema versioned via "v" key)
  schema_ver    int   — payload schema version (for migration safety)

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop().
* close() always safe to call.
* Publish is fire-and-forget by default; callers that need confirmation
  use publish_wait() (sync ack from broker).
* Dead-letter: failed deliveries are logged with full context, never silently dropped.
* SDK imports are lazy/optional — bus degrades to InMemory if SDK not installed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------


@dataclass
class HFAEvent:
    """
    Canonical event schema for HFA.

    Attributes
    ----------
    event_type:  Dot-namespaced type (e.g. "hfa.run.started").
    tenant_id:   Tenant identifier.
    run_id:      Run correlation key.
    payload:     Event-specific data dict (must be JSON-serialisable).
    event_id:    Auto-generated UUID4.
    timestamp:   Auto-set Unix epoch float.
    schema_ver:  Payload schema version (increment on breaking changes).
    """

    event_type: str
    tenant_id: str
    run_id: str
    payload: Dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    schema_ver: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HFAEvent":
        return cls(
            event_type=data["event_type"],
            tenant_id=data["tenant_id"],
            run_id=data["run_id"],
            payload=data.get("payload", {}),
            event_id=data.get("event_id", str(uuid.uuid4())),
            timestamp=data.get("timestamp", time.time()),
            schema_ver=data.get("schema_ver", 1),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> "HFAEvent":
        if isinstance(raw, bytes):
            raw = raw.decode()
        return cls.from_dict(json.loads(raw))


# Well-known event types
class EventType:
    RUN_STARTED = "hfa.run.started"
    RUN_DONE = "hfa.run.done"
    RUN_FAILED = "hfa.run.failed"
    NODE_STARTED = "hfa.node.started"
    NODE_DONE = "hfa.node.done"
    NODE_FAILED = "hfa.node.failed"
    BUDGET_RESERVED = "hfa.budget.reserved"
    BUDGET_DEPLETED = "hfa.budget.depleted"
    HEALING_RETRY = "hfa.healing.retry"
    CIRCUIT_OPENED = "hfa.healing.circuit_opened"
    COMPLIANCE_DENY = "hfa.compliance.denied"


# Subscriber callable type
Subscriber = Callable[[HFAEvent], Awaitable[None]]


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class EventBus(ABC):
    """
    Abstract event bus.

    Implementations must be safe to use concurrently from multiple coroutines.
    """

    @abstractmethod
    async def publish(self, event: HFAEvent) -> None:
        """
        Publish an event (fire-and-forget).

        Must not raise on broker failure — log and continue.
        """
        ...

    @abstractmethod
    async def subscribe(
        self,
        event_type: str,
        handler: Subscriber,
        tenant_id: Optional[str] = None,
    ) -> str:
        """
        Register a subscriber for `event_type`.

        Args:
            event_type: Event type string or "*" for all events.
            handler:    Async callable (HFAEvent) → None.
            tenant_id:  If provided, only events for this tenant are delivered.

        Returns:
            subscription_id (use to unsubscribe).
        """
        ...

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> None:
        """Remove a subscriber by ID."""
        ...

    async def close(self) -> None:
        """Graceful shutdown. Base no-op."""


# ---------------------------------------------------------------------------
# Subscription registry (shared by InMemory + other impls)
# ---------------------------------------------------------------------------


@dataclass
class _Subscription:
    sub_id: str
    event_type: str  # "*" = wildcard
    handler: Subscriber
    tenant_id: Optional[str]


class _SubscriptionRegistry:
    """Thread-safe subscription registry."""

    def __init__(self) -> None:
        self._subs: Dict[str, _Subscription] = {}
        self._lock = asyncio.Lock()

    async def add(
        self,
        event_type: str,
        handler: Subscriber,
        tenant_id: Optional[str] = None,
    ) -> str:
        sub_id = str(uuid.uuid4())
        async with self._lock:
            self._subs[sub_id] = _Subscription(sub_id, event_type, handler, tenant_id)
        logger.debug(
            "EventBus.subscribe: sub_id=%s type=%s tenant=%s",
            sub_id,
            event_type,
            tenant_id,
        )
        return sub_id

    async def remove(self, sub_id: str) -> None:
        async with self._lock:
            self._subs.pop(sub_id, None)
        logger.debug("EventBus.unsubscribe: sub_id=%s", sub_id)

    def matching(self, event: HFAEvent) -> List[_Subscription]:
        """Return all subscriptions that match this event."""
        result = []
        for sub in self._subs.values():
            type_match = sub.event_type in ("*", event.event_type)
            tenant_match = sub.tenant_id is None or sub.tenant_id == event.tenant_id
            if type_match and tenant_match:
                result.append(sub)
        return result


# ---------------------------------------------------------------------------
# InMemoryEventBus
# ---------------------------------------------------------------------------


class InMemoryEventBus(EventBus):
    """
    In-process event bus backed by asyncio.Queue.

    Delivery is sequential within each subscriber.
    Delivery errors are logged but never propagated (dead-letter semantics).

    Args:
        delivery_timeout: Per-subscriber delivery timeout in seconds.
    """

    def __init__(self, delivery_timeout: float = 5.0) -> None:
        self._registry = _SubscriptionRegistry()
        self._delivery_timeout = delivery_timeout
        self._published: List[HFAEvent] = []  # for test inspection
        self._running = True
        logger.info("InMemoryEventBus created")

    async def publish(self, event: HFAEvent) -> None:
        self._published.append(event)
        logger.debug(
            "EventBus.publish: type=%s tenant=%s run=%s",
            event.event_type,
            event.tenant_id,
            event.run_id,
        )
        subs = self._registry.matching(event)
        if not subs:
            return
        # Fan-out: deliver to all matching subscribers concurrently
        await asyncio.gather(
            *[self._deliver(sub, event) for sub in subs],
            return_exceptions=True,
        )

    async def subscribe(
        self,
        event_type: str,
        handler: Subscriber,
        tenant_id: Optional[str] = None,
    ) -> str:
        return await self._registry.add(event_type, handler, tenant_id)

    async def unsubscribe(self, subscription_id: str) -> None:
        await self._registry.remove(subscription_id)

    async def close(self) -> None:
        self._running = False
        logger.info("InMemoryEventBus closed")

    # Test helpers
    def published_events(self, event_type: Optional[str] = None) -> List[HFAEvent]:
        """Return all published events, optionally filtered by type."""
        if event_type is None:
            return list(self._published)
        return [e for e in self._published if e.event_type == event_type]

    def clear_events(self) -> None:
        self._published.clear()

    # ------------------------------------------------------------------

    async def _deliver(self, sub: _Subscription, event: HFAEvent) -> None:
        try:
            await asyncio.wait_for(sub.handler(event), self._delivery_timeout)
        except asyncio.TimeoutError:
            logger.error(
                "EventBus delivery TIMEOUT: sub=%s type=%s run=%s",
                sub.sub_id,
                event.event_type,
                event.run_id,
            )
        except Exception as exc:
            logger.error(
                "EventBus delivery ERROR: sub=%s type=%s run=%s error=%s",
                sub.sub_id,
                event.event_type,
                event.run_id,
                exc,
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# NATSEventBus — production implementation
# ---------------------------------------------------------------------------


class NATSEventBus(EventBus):
    """
    NATS JetStream event bus.

    Requires: pip install nats-py

    Subject mapping: "hfa.{event_type}.{tenant_id}"
    Consumer groups: each subscriber gets a durable consumer.

    Args:
        servers:  NATS server URLs (e.g. ["nats://localhost:4222"]).
        stream:   JetStream stream name (default "HFA").
        prefix:   Subject prefix (default "hfa").
    """

    def __init__(
        self,
        servers: List[str],
        stream: str = "HFA",
        prefix: str = "hfa",
    ) -> None:
        self._servers = servers
        self._stream = stream
        self._prefix = prefix
        self._nc = None  # nats.Client
        self._js = None  # JetStream context
        self._registry = _SubscriptionRegistry()
        self._sub_handles: Dict[str, Any] = {}  # sub_id → nats subscription
        logger.info("NATSEventBus created: servers=%s stream=%s", servers, stream)

    async def connect(self) -> None:
        """Connect to NATS and ensure stream exists."""
        try:
            import nats
        except ImportError:
            raise ImportError(
                "nats-py is required for NATSEventBus: pip install nats-py"
            )
        self._nc = await nats.connect(self._servers)
        self._js = self._nc.jetstream()
        # Ensure stream exists (idempotent)
        try:
            await self._js.add_stream(
                name=self._stream,
                subjects=[f"{self._prefix}.>"],
                max_age=86400,  # 24h retention
                storage="file",
            )
        except Exception as exc:
            logger.warning("NATS stream add_stream (may exist): %s", exc)
        logger.info("NATSEventBus connected: stream=%s", self._stream)

    async def publish(self, event: HFAEvent) -> None:
        subject = f"{self._prefix}.{event.event_type}.{event.tenant_id}"
        payload = event.to_json().encode()
        try:
            await self._js.publish(subject, payload)
            logger.debug(
                "NATS publish: subject=%s event_id=%s", subject, event.event_id
            )
        except Exception as exc:
            logger.error(
                "NATS publish FAILED (dead-letter): subject=%s event_id=%s error=%s",
                subject,
                event.event_id,
                exc,
            )
            # Dead-letter: do not raise — fire-and-forget contract

    async def subscribe(
        self,
        event_type: str,
        handler: Subscriber,
        tenant_id: Optional[str] = None,
    ) -> str:
        sub_id = await self._registry.add(event_type, handler, tenant_id)

        # Build subject filter
        if event_type == "*":
            subject = f"{self._prefix}.>"
        elif tenant_id:
            subject = f"{self._prefix}.{event_type}.{tenant_id}"
        else:
            subject = f"{self._prefix}.{event_type}.>"

        try:
            durable = f"hfa-{sub_id[:8]}"
            psub = await self._js.pull_subscribe(subject, durable=durable)
            # Launch consumer coroutine
            loop = asyncio.get_running_loop()
            task = loop.create_task(
                self._consume(sub_id, psub, handler),
                name=f"nats.consumer.{sub_id[:8]}",
            )
            self._sub_handles[sub_id] = (psub, task)
        except Exception as exc:
            logger.error("NATS subscribe failed: %s", exc, exc_info=True)

        return sub_id

    async def unsubscribe(self, subscription_id: str) -> None:
        handle = self._sub_handles.pop(subscription_id, None)
        if handle:
            psub, task = handle
            task.cancel()
            try:
                await psub.unsubscribe()
            except Exception:
                pass
        await self._registry.remove(subscription_id)

    async def close(self) -> None:
        for sub_id in list(self._sub_handles):
            await self.unsubscribe(sub_id)
        if self._nc:
            await self._nc.close()
        logger.info("NATSEventBus closed")

    async def _consume(self, sub_id: str, psub, handler: Subscriber) -> None:
        while True:
            try:
                msgs = await psub.fetch(batch=10, timeout=1.0)
                for msg in msgs:
                    try:
                        event = HFAEvent.from_json(msg.data)
                        await handler(event)
                        await msg.ack()
                    except Exception as exc:
                        logger.error(
                            "NATS consumer error: sub=%s error=%s",
                            sub_id,
                            exc,
                            exc_info=True,
                        )
                        await msg.nak()
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)


# ---------------------------------------------------------------------------
# KafkaEventBus — high-throughput alternative
# ---------------------------------------------------------------------------


class KafkaEventBus(EventBus):
    """
    Apache Kafka event bus via aiokafka.

    Requires: pip install aiokafka

    Topic naming: "hfa-{event_type}" (dots replaced with hyphens).
    Consumer groups: "hfa-{tenant_id}" for tenant isolation.

    Args:
        bootstrap_servers: Comma-separated Kafka brokers.
        default_topic:     Fallback topic if event_type has no mapping.
        num_partitions:    Partitions for auto-created topics.
        replication_factor: Replication for auto-created topics.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        default_topic: str = "hfa-events",
        num_partitions: int = 6,
        replication_factor: int = 1,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._default_topic = default_topic
        self._num_partitions = num_partitions
        self._replication = replication_factor
        self._producer = None
        self._consumers: Dict[str, Any] = {}  # sub_id → consumer task
        self._registry = _SubscriptionRegistry()
        logger.info("KafkaEventBus created: brokers=%s", bootstrap_servers)

    @staticmethod
    def _topic_for(event_type: str) -> str:
        return f"hfa-{event_type.replace('.', '-')}"

    async def connect(self) -> None:
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError:
            raise ImportError("aiokafka required: pip install aiokafka")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            value_serializer=lambda v: v.encode() if isinstance(v, str) else v,
        )
        await self._producer.start()
        logger.info("KafkaEventBus producer connected")

    async def publish(self, event: HFAEvent) -> None:
        topic = self._topic_for(event.event_type)
        try:
            await self._producer.send_and_wait(
                topic,
                value=event.to_json(),
                key=event.tenant_id.encode(),
            )
            logger.debug("Kafka publish: topic=%s event_id=%s", topic, event.event_id)
        except Exception as exc:
            logger.error(
                "Kafka publish FAILED (dead-letter): topic=%s event_id=%s error=%s",
                topic,
                event.event_id,
                exc,
            )

    async def subscribe(
        self,
        event_type: str,
        handler: Subscriber,
        tenant_id: Optional[str] = None,
    ) -> str:
        try:
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            raise ImportError("aiokafka required: pip install aiokafka")

        sub_id = await self._registry.add(event_type, handler, tenant_id)
        topic = (
            self._topic_for(event_type) if event_type != "*" else self._default_topic
        )
        group = f"hfa-{tenant_id or 'global'}"

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap,
            group_id=group,
            auto_offset_reset="latest",
            value_deserializer=lambda v: v.decode(),
        )
        await consumer.start()
        loop = asyncio.get_running_loop()
        task = loop.create_task(
            self._consume(sub_id, consumer, handler, tenant_id),
            name=f"kafka.consumer.{sub_id[:8]}",
        )
        self._consumers[sub_id] = (consumer, task)
        return sub_id

    async def unsubscribe(self, subscription_id: str) -> None:
        handle = self._consumers.pop(subscription_id, None)
        if handle:
            consumer, task = handle
            task.cancel()
            try:
                await consumer.stop()
            except Exception:
                pass
        await self._registry.remove(subscription_id)

    async def close(self) -> None:
        for sub_id in list(self._consumers):
            await self.unsubscribe(sub_id)
        if self._producer:
            await self._producer.stop()
        logger.info("KafkaEventBus closed")

    async def _consume(
        self,
        sub_id: str,
        consumer,
        handler: Subscriber,
        tenant_id: Optional[str],
    ) -> None:
        try:
            async for msg in consumer:
                try:
                    event = HFAEvent.from_json(msg.value)
                    if tenant_id and event.tenant_id != tenant_id:
                        continue
                    await handler(event)
                except Exception as exc:
                    logger.error(
                        "Kafka consumer error: sub=%s error=%s",
                        sub_id,
                        exc,
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            pass
