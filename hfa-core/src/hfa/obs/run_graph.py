"""
hfa-core/src/hfa/obs/run_graph.py
IRONCLAD Sprint 5 — ExecutionGraph

Purpose
-------
Track the directed acyclic graph (DAG) of agent calls within a single run.
Each node represents one agent invocation; edges represent data dependencies
(output of A is input to B).

Design goals
------------
* Immutable nodes after commit — add_node() creates, commit_node() finalises.
* Serializable to JSON for audit log and Jaeger baggage.
* Thread-safe via asyncio.Lock.
* No external dependencies — pure Python dataclasses + stdlib.
* Graceful handling of cycles (raises ExecutionGraphError, not silently ignored).

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop().
* close() is always safe to call.

Node lifecycle
--------------
  PENDING   → created, not yet started
  RUNNING   → execution started (start_node called)
  DONE      → completed successfully (commit_node called)
  FAILED    → failed (fail_node called)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Node status
# ---------------------------------------------------------------------------


class NodeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# GraphNode
# ---------------------------------------------------------------------------


@dataclass
class GraphNode:
    """
    Single node in the execution graph.

    Attributes
    ----------
    node_id:        Unique identifier (e.g. "architect:0", "coder:1").
    agent_type:     AgentType string (e.g. "architect").
    run_id:         Parent run identifier.
    tenant_id:      Tenant identifier.
    depth:          DAG depth (root = 0).
    parent_ids:     IDs of nodes this node depends on.
    status:         Current lifecycle status.
    started_at:     Unix timestamp when execution started (None if not started).
    finished_at:    Unix timestamp when execution completed (None if not done).
    input_hash:     SHA-256 of serialised input (for cache dedup).
    output_hash:    SHA-256 of serialised output (set on commit).
    tokens_used:    LLM tokens consumed by this node.
    cost_cents:     Cost in INTEGER CENTS for this node.
    error:          Error message if status == FAILED.
    metadata:       Arbitrary key-value metadata (schema version, model, etc.).
    """

    node_id: str
    agent_type: str
    run_id: str
    tenant_id: str
    depth: int = 0
    parent_ids: List[str] = field(default_factory=list)
    status: NodeStatus = NodeStatus.PENDING
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    input_hash: Optional[str] = None
    output_hash: Optional[str] = None
    tokens_used: int = 0
    cost_cents: int = 0  # ✅ integer cents
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> Optional[float]:
        """Wall-clock duration in milliseconds, or None if not finished."""
        if self.started_at is None or self.finished_at is None:
            return None
        return (self.finished_at - self.started_at) * 1000

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to plain dict (JSON-compatible)."""
        d = asdict(self)
        d["status"] = self.status.value
        d["duration_ms"] = self.duration_ms
        return d


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ExecutionGraphError(Exception):
    """Raised on invalid graph operations."""


# ---------------------------------------------------------------------------
# ExecutionGraph
# ---------------------------------------------------------------------------


class ExecutionGraph:
    """
    Directed acyclic graph of agent invocations for a single run.

    Thread-safe: all mutating operations acquire ``_lock``.

    Usage
    -----
        graph = ExecutionGraph(run_id="run-acme-abc", tenant_id="acme_corp")

        nid = graph.add_node("architect", parent_ids=[])
        await graph.start_node(nid)
        await graph.commit_node(nid, tokens=200, cost_cents=8, output=manifest)

        cid = graph.add_node("coder", parent_ids=[nid])
        await graph.start_node(cid)
        await graph.commit_node(cid, tokens=500, cost_cents=20, output=code_set)

        snapshot = graph.snapshot()
        print(snapshot["summary"])

    Args:
        run_id:    Run identifier (stored on each node).
        tenant_id: Tenant identifier (stored on each node).
    """

    def __init__(self, run_id: str, tenant_id: str) -> None:
        self.run_id = run_id
        self.tenant_id = tenant_id
        self._nodes: Dict[str, GraphNode] = {}
        self._edges: Dict[str, Set[str]] = {}  # node_id → set of child node_ids
        self._lock = asyncio.Lock()
        self._seq = 0
        logger.debug("ExecutionGraph created: run=%s tenant=%s", run_id, tenant_id)

    # ------------------------------------------------------------------
    # Node management
    # ------------------------------------------------------------------

    def add_node(
        self,
        agent_type: str,
        parent_ids: Optional[List[str]] = None,
        input_data: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add a new PENDING node to the graph.

        Cycle detection: raises ExecutionGraphError if any parent_id is not
        already a known node, or if adding this edge would create a cycle.

        Args:
            agent_type:  AgentType string.
            parent_ids:  List of node IDs this node depends on.
            input_data:  Serialisable input (hashed for dedup).
            metadata:    Optional metadata dict.

        Returns:
            node_id string (e.g. "architect:0").

        Raises:
            ExecutionGraphError: If parent_id unknown or cycle detected.
        """
        parents = parent_ids or []

        # Validate parents exist
        for pid in parents:
            if pid not in self._nodes:
                raise ExecutionGraphError(
                    f"Parent node {pid!r} not found in graph for run={self.run_id}"
                )

        node_id = f"{agent_type}:{self._seq}"
        self._seq += 1

        # Compute depth from max parent depth
        depth = 0
        for pid in parents:
            depth = max(depth, self._nodes[pid].depth + 1)

        # Hash input if provided
        input_hash = None
        if input_data is not None:
            try:
                raw = json.dumps(input_data, sort_keys=True, default=str)
                input_hash = hashlib.sha256(raw.encode()).hexdigest()
            except Exception:
                input_hash = None

        node = GraphNode(
            node_id=node_id,
            agent_type=agent_type,
            run_id=self.run_id,
            tenant_id=self.tenant_id,
            depth=depth,
            parent_ids=parents,
            input_hash=input_hash,
            metadata=metadata or {},
        )
        self._nodes[node_id] = node

        # Register edges (parent → this node)
        for pid in parents:
            self._edges.setdefault(pid, set()).add(node_id)

        logger.debug(
            "Graph.add_node: run=%s node=%s depth=%d parents=%s",
            self.run_id,
            node_id,
            depth,
            parents,
        )
        return node_id

    async def start_node(self, node_id: str) -> None:
        """
        Transition node from PENDING → RUNNING.

        Raises:
            ExecutionGraphError: If node not found or not in PENDING state.
        """
        async with self._lock:
            node = self._get_node(node_id)
            if node.status != NodeStatus.PENDING:
                raise ExecutionGraphError(
                    f"Cannot start node {node_id!r}: status={node.status.value}"
                )
            node.status = NodeStatus.RUNNING
            node.started_at = time.time()
        logger.debug("Graph.start_node: run=%s node=%s", self.run_id, node_id)

    async def commit_node(
        self,
        node_id: str,
        tokens: int = 0,
        cost_cents: int = 0,
        output: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Transition node from RUNNING → DONE.

        Args:
            node_id:    Node to commit.
            tokens:     LLM tokens consumed.
            cost_cents: Cost in INTEGER CENTS.
            output:     Serialisable output (hashed for audit).
            metadata:   Additional metadata to merge.

        Raises:
            ExecutionGraphError: If node not found or not in RUNNING state.
        """
        async with self._lock:
            node = self._get_node(node_id)
            if node.status != NodeStatus.RUNNING:
                raise ExecutionGraphError(
                    f"Cannot commit node {node_id!r}: status={node.status.value}"
                )
            node.status = NodeStatus.DONE
            node.finished_at = time.time()
            node.tokens_used = tokens
            node.cost_cents = cost_cents

            if output is not None:
                try:
                    raw = json.dumps(output, sort_keys=True, default=str)
                    node.output_hash = hashlib.sha256(raw.encode()).hexdigest()
                except Exception:
                    node.output_hash = None

            if metadata:
                node.metadata.update(metadata)

        logger.debug(
            "Graph.commit_node: run=%s node=%s tokens=%d cost=%d¢ duration=%.0fms",
            self.run_id,
            node_id,
            tokens,
            cost_cents,
            (self._nodes[node_id].duration_ms or 0),
        )

    async def fail_node(
        self,
        node_id: str,
        error: str,
    ) -> None:
        """
        Transition node from RUNNING → FAILED.

        Args:
            node_id: Node to fail.
            error:   Error message string.

        Raises:
            ExecutionGraphError: If node not found or not in RUNNING state.
        """
        async with self._lock:
            node = self._get_node(node_id)
            if node.status != NodeStatus.RUNNING:
                raise ExecutionGraphError(
                    f"Cannot fail node {node_id!r}: status={node.status.value}"
                )
            node.status = NodeStatus.FAILED
            node.finished_at = time.time()
            node.error = error
        logger.warning(
            "Graph.fail_node: run=%s node=%s error=%s", self.run_id, node_id, error
        )

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_node(self, node_id: str) -> Optional[GraphNode]:
        """Return node by ID (read-only), or None if not found."""
        return self._nodes.get(node_id)

    def get_nodes_by_status(self, status: NodeStatus) -> List[GraphNode]:
        """Return all nodes with a given status."""
        return [n for n in self._nodes.values() if n.status == status]

    def get_children(self, node_id: str) -> List[GraphNode]:
        """Return direct children of a node."""
        child_ids = self._edges.get(node_id, set())
        return [self._nodes[cid] for cid in child_ids if cid in self._nodes]

    def total_cost_cents(self) -> int:
        """Sum of cost_cents across all DONE nodes."""
        return sum(
            n.cost_cents for n in self._nodes.values() if n.status == NodeStatus.DONE
        )

    def total_tokens(self) -> int:
        """Sum of tokens_used across all DONE nodes."""
        return sum(
            n.tokens_used for n in self._nodes.values() if n.status == NodeStatus.DONE
        )

    def is_complete(self) -> bool:
        """True if all nodes are DONE or FAILED (none PENDING or RUNNING)."""
        return all(
            n.status in (NodeStatus.DONE, NodeStatus.FAILED)
            for n in self._nodes.values()
        )

    def has_failures(self) -> bool:
        """True if any node is in FAILED status."""
        return any(n.status == NodeStatus.FAILED for n in self._nodes.values())

    def snapshot(self) -> Dict[str, Any]:
        """
        Return a JSON-serialisable snapshot of the full graph.

        Shape
        -----
        {
            "run_id":   ...,
            "tenant_id": ...,
            "nodes":    [GraphNode.to_dict(), ...],
            "edges":    {"node_id": ["child_id", ...], ...},
            "summary":  {
                "total_nodes":   int,
                "done":          int,
                "failed":        int,
                "running":       int,
                "pending":       int,
                "total_tokens":  int,
                "total_cost_cents": int,
                "is_complete":   bool,
                "has_failures":  bool,
            }
        }
        """
        nodes = [n.to_dict() for n in self._nodes.values()]
        edges = {k: list(v) for k, v in self._edges.items()}

        by_status = {s: 0 for s in NodeStatus}
        for n in self._nodes.values():
            by_status[n.status] += 1

        return {
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "nodes": nodes,
            "edges": edges,
            "summary": {
                "total_nodes": len(nodes),
                "done": by_status[NodeStatus.DONE],
                "failed": by_status[NodeStatus.FAILED],
                "running": by_status[NodeStatus.RUNNING],
                "pending": by_status[NodeStatus.PENDING],
                "total_tokens": self.total_tokens(),
                "total_cost_cents": self.total_cost_cents(),
                "is_complete": self.is_complete(),
                "has_failures": self.has_failures(),
            },
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        """Serialise graph snapshot to JSON string."""
        return json.dumps(self.snapshot(), indent=indent)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_node(self, node_id: str) -> GraphNode:
        node = self._nodes.get(node_id)
        if node is None:
            raise ExecutionGraphError(
                f"Node {node_id!r} not found in graph for run={self.run_id}"
            )
        return node
