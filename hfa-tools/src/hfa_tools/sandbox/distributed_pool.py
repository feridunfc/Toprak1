"""
hfa-tools/src/hfa_tools/sandbox/distributed_pool.py
IRONCLAD Sprint 7 Faz 2 + Sprint 8 Mini 3 — DistributedSandboxPool + Tracing

Sprint 7: HFAMetrics slot telemetry
Sprint 8 Mini 3: OTel tracing
  ✅ hfa.sandbox.execute span in execute() — per-execution span
  ✅ Attributes: hfa.run_id, hfa.language, hfa.node_id
  ✅ Success: exit_code attribute set, span status OK
  ✅ Failure: exception recorded, span status ERROR, node degraded
  ✅ Slot metrics (inc/dec) still in finally — tracing does not change slot contract

IRONCLAD rules
--------------
* No print() -- logging only.
* No asyncio.get_event_loop() -- get_running_loop() where needed.
* Slot counter always released in finally (no leaks on failure).
* Fail-closed: SandboxClusterError when no healthy nodes.
* close() always safe.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from hfa.obs.metrics import HFAMetrics  # Sprint 7 Faz 2
from hfa.obs.tracing import get_tracer, HFATracing  # Sprint 8 Mini 3

logger = logging.getLogger(__name__)
_tracer = get_tracer("hfa.sandbox")


# ---------------------------------------------------------------------------
# Node model
# ---------------------------------------------------------------------------


class NodeHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class SandboxNode:
    """Single node in the distributed sandbox cluster."""

    node_id: str
    host: str
    port: int = 2376
    capacity: int = 10
    languages: List[str] = field(default_factory=lambda: ["python", "node"])
    health: NodeHealth = NodeHealth.HEALTHY
    active: int = 0
    last_seen: float = field(default_factory=time.time)
    weight: int = 10
    labels: Dict[str, str] = field(default_factory=dict)

    @property
    def available_slots(self) -> int:
        return max(0, self.capacity - self.active)

    @property
    def is_available(self) -> bool:
        return self.health == NodeHealth.HEALTHY and self.available_slots > 0

    @property
    def load_factor(self) -> float:
        if self.capacity == 0:
            return 1.0
        return self.active / self.capacity

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["health"] = self.health.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SandboxNode":
        d = dict(data)
        d["health"] = NodeHealth(d.get("health", "healthy"))
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Execution result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SandboxResult:
    run_id: str
    node_id: str
    language: str
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: float
    timed_out: bool = False

    @property
    def success(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class SandboxClusterError(Exception):
    """No healthy nodes available (fail-closed)."""

    def __init__(self, language: str, node_count: int) -> None:
        super().__init__(f"No healthy nodes for language={language!r} (total={node_count})")
        self.language = language
        self.node_count = node_count


class SandboxExecutionError(Exception):
    """Execution failed on the selected node."""


# ---------------------------------------------------------------------------
# NodeSelector
# ---------------------------------------------------------------------------


class NodeSelector:
    """
    Weighted round-robin node selection.
    Filter: health==HEALTHY AND language supported.
    Sort: load_factor ASC, then node_id ASC (stable tie-break).
    Among equal load_factor, prefer higher weight.
    """

    def select(self, nodes: List[SandboxNode], language: str) -> SandboxNode:
        eligible = [n for n in nodes if n.is_available and language in n.languages]
        if not eligible:
            raise SandboxClusterError(language, len(nodes))
        eligible.sort(key=lambda n: (n.load_factor, -n.weight, n.node_id))
        return eligible[0]


# ---------------------------------------------------------------------------
# NodeHealthMonitor
# ---------------------------------------------------------------------------


class NodeHealthMonitor:
    """Background task: HEALTHY -> DEGRADED -> UNHEALTHY based on heartbeat age."""

    def __init__(self, stale_threshold: float = 30.0, check_interval: float = 10.0) -> None:
        self._stale = stale_threshold
        self._degraded = stale_threshold / 2
        self._interval = check_interval
        self._task: Optional[asyncio.Task] = None
        self._nodes: Dict[str, SandboxNode] = {}

    def start(self, nodes: Dict[str, SandboxNode]) -> None:
        self._nodes = nodes
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop(), name="sandbox.health_monitor")
        logger.info("NodeHealthMonitor started")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._interval)
                now = time.time()
                for node in self._nodes.values():
                    age = now - node.last_seen
                    if age > self._stale:
                        if node.health != NodeHealth.UNHEALTHY:
                            logger.warning("Node UNHEALTHY: %s age=%.0fs", node.node_id, age)
                        node.health = NodeHealth.UNHEALTHY
                    elif age > self._degraded:
                        if node.health == NodeHealth.HEALTHY:
                            logger.warning("Node DEGRADED: %s age=%.0fs", node.node_id, age)
                        node.health = NodeHealth.DEGRADED
                    else:
                        if node.health != NodeHealth.HEALTHY:
                            logger.info("Node RECOVERED: %s", node.node_id)
                        node.health = NodeHealth.HEALTHY
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("NodeHealthMonitor error: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# DistributedSandboxPool
# ---------------------------------------------------------------------------


class DistributedSandboxPool:
    """
    Cluster-aware sandbox pool with Redis node discovery.

    Sprint 7 additions:
    * HFAMetrics.inc_sandbox_slots() on slot reservation.
    * HFAMetrics.dec_sandbox_slots() in finally block (never skipped).
    """

    REGISTRY_KEY = "sandbox:nodes"

    def __init__(
        self,
        redis_client=None,
        heartbeat_interval: float = 15.0,
        stale_threshold: float = 30.0,
        local_node: Optional[SandboxNode] = None,
    ) -> None:
        self._redis = redis_client
        self._heartbeat_interval = heartbeat_interval
        self._nodes: Dict[str, SandboxNode] = {}
        self._selector = NodeSelector()
        self._monitor = NodeHealthMonitor(stale_threshold=stale_threshold)
        self._lock = asyncio.Lock()
        self._running = False
        self._discovery_task: Optional[asyncio.Task] = None

        if local_node:
            self._nodes[local_node.node_id] = local_node

    async def start(self) -> None:
        self._running = True
        self._monitor.start(self._nodes)
        if self._redis:
            loop = asyncio.get_running_loop()
            self._discovery_task = loop.create_task(
                self._discovery_loop(), name="sandbox.discovery"
            )
        logger.info("DistributedSandboxPool started")

    async def close(self) -> None:
        self._running = False
        await self._monitor.stop()
        if self._discovery_task:
            self._discovery_task.cancel()
            try:
                await self._discovery_task
            except asyncio.CancelledError:
                pass
        logger.info("DistributedSandboxPool closed")

    # ------------------------------------------------------------------
    # Node management
    # ------------------------------------------------------------------

    async def register_node(self, node: SandboxNode) -> None:
        async with self._lock:
            self._nodes[node.node_id] = node
        if self._redis:
            await self._publish_node(node)
        logger.info("SandboxNode registered: %s host=%s", node.node_id, node.host)

    async def deregister_node(self, node_id: str) -> None:
        async with self._lock:
            self._nodes.pop(node_id, None)
        if self._redis:
            try:
                await self._redis.hdel(self.REGISTRY_KEY, node_id)
            except Exception as exc:
                logger.error("Redis deregister error: %s", exc)

    def list_nodes(self) -> List[SandboxNode]:
        return list(self._nodes.values())

    def healthy_node_count(self) -> int:
        return sum(1 for n in self._nodes.values() if n.health == NodeHealth.HEALTHY)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(
        self,
        run_id: str,
        language: str,
        code: str,
        timeout: int = 30,
        stdin: Optional[str] = None,
    ) -> SandboxResult:
        """
        Execute code on the best available node.

        Slot accounting:
          node.active and HFAMetrics slot counter are BOTH in try/finally.
          They cannot be skipped on failure (no slot leak).
        """
        async with self._lock:
            nodes = list(self._nodes.values())

        node = self._selector.select(nodes, language)

        # ── Reserve slot ────────────────────────────────────────────────
        async with self._lock:
            node.active += 1
        HFAMetrics.inc_sandbox_slots(node.node_id)

        with _tracer.start_as_current_span("hfa.sandbox.execute") as span:
            HFATracing.set_attrs(
                span,
                {
                    "hfa.run_id": run_id,
                    "hfa.language": language,
                    "hfa.node_id": node.node_id,
                },
            )

            try:
                result = await self._run_on_node(node, run_id, language, code, timeout, stdin)
                HFATracing.set_attrs(span, {"hfa.exit_code": result.exit_code})
                HFATracing.span_ok(span)
                logger.info(
                    "Sandbox execute: run=%s node=%s exit=%d duration=%.0fms",
                    run_id,
                    node.node_id,
                    result.exit_code,
                    result.duration_ms,
                )
                return result

            except Exception as exc:
                async with self._lock:
                    node.health = NodeHealth.DEGRADED
                HFATracing.record_exc(span, exc)
                logger.error(
                    "Sandbox execute FAILED: run=%s node=%s: %s", run_id, node.node_id, exc
                )
                raise SandboxExecutionError(str(exc)) from exc

            finally:
                # ── Release slot — always runs (success AND failure) ───
                async with self._lock:
                    node.active = max(0, node.active - 1)
                HFAMetrics.dec_sandbox_slots(node.node_id)

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    async def _run_on_node(
        self,
        node: SandboxNode,
        run_id: str,
        language: str,
        code: str,
        timeout: int,
        stdin: Optional[str],
    ) -> SandboxResult:
        loop = asyncio.get_running_loop()
        start = time.time()
        timed_out = False
        try:
            stdout, stderr, exit_code = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self._docker_exec(node, language, code, stdin),
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            timed_out, stdout, stderr, exit_code = True, "", "Execution timeout", 124

        return SandboxResult(
            run_id=run_id,
            node_id=node.node_id,
            language=language,
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            duration_ms=(time.time() - start) * 1000,
            timed_out=timed_out,
        )

    @staticmethod
    def _docker_exec(node: SandboxNode, language: str, code: str, stdin: Optional[str]) -> tuple:
        import subprocess
        import tempfile
        import os

        interp = {"python": "python3", "node": "node"}.get(language, language)
        suffix = {"python": ".py", "node": ".js"}.get(language, ".txt")
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
            f.write(code)
            fpath = f.name
        try:
            r = subprocess.run(
                [interp, fpath],
                capture_output=True,
                text=True,
                input=stdin,
                timeout=30,
            )
            return r.stdout, r.stderr, r.returncode
        except subprocess.TimeoutExpired:
            return "", "Execution timeout", 124
        except FileNotFoundError:
            return "", f"Runtime not found: {interp}", 127
        finally:
            try:
                os.unlink(fpath)
            except Exception:
                pass

    async def _discovery_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                raw = await self._redis.hgetall(self.REGISTRY_KEY)
                if raw:
                    async with self._lock:
                        for node_id, data in raw.items():
                            nid = node_id.decode() if isinstance(node_id, bytes) else node_id
                            try:
                                d = json.loads(data.decode() if isinstance(data, bytes) else data)
                                if nid in self._nodes:
                                    self._nodes[nid].last_seen = d.get("last_seen", time.time())
                                else:
                                    self._nodes[nid] = SandboxNode.from_dict(d)
                                    logger.info("Discovered node: %s", nid)
                            except Exception as exc:
                                logger.error("Node parse error %s: %s", nid, exc)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Discovery loop error: %s", exc, exc_info=True)

    async def _publish_node(self, node: SandboxNode) -> None:
        try:
            await self._redis.hset(self.REGISTRY_KEY, node.node_id, json.dumps(node.to_dict()))
            await self._redis.expire(self.REGISTRY_KEY, 300)
        except Exception as exc:
            logger.error("Redis node publish error: %s", exc)
