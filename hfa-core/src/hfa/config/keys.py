"""
hfa-core/src/hfa/config/keys.py
IRONCLAD Sprint 15 — Centralized Redis key builders and TTL constants

Design
------
All Redis key patterns and TTL values are defined here. No raw
string literals or magic integers should appear in source files
for the keys listed below.

Usage
-----
    from hfa.config.keys import RedisKey, RedisTTL

    key = RedisKey.run_state("run-abc-123")
    # → "hfa:run:state:run-abc-123"

    await redis.set(key, "admitted", ex=RedisTTL.RUN_STATE)

Prefix policy
-------------
All keys are prefixed with the value of RedisKey.PREFIX ("hfa").
To migrate the prefix (e.g. for multi-tenant namespacing), change
PREFIX in one place. All callers update automatically.

Key layout reference
--------------------
  hfa:run:state:{run_id}           STR  — run lifecycle state
  hfa:run:meta:{run_id}            HASH — run metadata
  hfa:run:result:{run_id}          HASH — execution result
  hfa:run:claim:{run_id}           STR  — claim owner + expiry
  hfa:run:cancel:{run_id}          STR  — cancellation flag
  hfa:run:payload:{run_id}         STR  — original payload (DLQ replay)

  hfa:tenant:{id}:config           HASH — tenant settings
  hfa:tenant:{id}:inflight         STR  — in-flight run counter (INT)
  hfa:tenant:{id}:rate             ZSET — sliding-window rate entries

  hfa:cp:running                   ZSET — active run_id → admitted_at score
  hfa:cp:leader                    STR  — current CP leader instance_id
  hfa:cp:fence                     STR  — leader fencing token (INCR)
  hfa:cp:dlq:meta:{run_id}         HASH — DLQ entry metadata
  hfa:cp:dlq:index                 ZSET — DLQ run_id → dead_lettered_at (future)

  hfa:worker:{id}                  HASH — worker profile / heartbeat
  hfa:stream:control               STREAM — admission + scheduling events
  hfa:stream:runs:{shard}          STREAM — per-shard run requests
  hfa:stream:results               STREAM — completion events
  hfa:stream:heartbeat             STREAM — worker heartbeat events
  hfa:stream:dlq                   STREAM — dead-lettered events
"""

from __future__ import annotations

from dataclasses import dataclass


# ---------------------------------------------------------------------------
# TTL constants (seconds)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RedisTTL:
    """
    All TTL values in seconds. Use these instead of hardcoded integers.

    Attributes
    ----------
    RUN_STATE:        Expiry for hfa:run:state:{run_id}. 24 hours.
    RUN_META:         Expiry for hfa:run:meta:{run_id}. 24 hours.
    RUN_RESULT:       Expiry for hfa:run:result:{run_id}. 24 hours.
    RUN_CLAIM:        Claim lock TTL. 5 minutes.
    DLQ_META:         DLQ entry retention. 7 days.
    TENANT_INFLIGHT:  Inflight counter TTL. 24 hours.
    TENANT_RATE:      Rate-limit sliding-window key TTL. 1 hour.
    STREAM_MAXLEN:    Approximate max entries in control stream.
    SHARD_MAXLEN:     Approximate max entries in shard run streams.
    """

    RUN_STATE: int = 86_400  # 24 h
    RUN_META: int = 86_400  # 24 h
    RUN_RESULT: int = 86_400  # 24 h
    RUN_CLAIM: int = 300  # 5 min
    DLQ_META: int = 604_800  # 7 days
    TENANT_INFLIGHT: int = 86_400  # 24 h
    TENANT_RATE: int = 3_600  # 1 h
    STREAM_MAXLEN: int = 100_000
    SHARD_MAXLEN: int = 50_000


# Singleton — import once, use everywhere.
TTL = RedisTTL()


# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------


class RedisKey:
    """
    Builds Redis key strings from semantic identifiers.

    All keys are prefixed with PREFIX ("hfa"). Change PREFIX here
    to rename the entire key namespace in one edit.

    Class methods are used (not instance) to avoid unnecessary
    instantiation while keeping the namespace clean.
    """

    PREFIX: str = "hfa"

    # ------------------------------------------------------------------
    # Run keys
    # ------------------------------------------------------------------

    @classmethod
    def run_state(cls, run_id: str) -> str:
        """hfa:run:state:{run_id}"""
        return f"{cls.PREFIX}:run:state:{run_id}"

    @classmethod
    def run_meta(cls, run_id: str) -> str:
        """hfa:run:meta:{run_id}"""
        return f"{cls.PREFIX}:run:meta:{run_id}"

    @classmethod
    def run_result(cls, run_id: str) -> str:
        """hfa:run:result:{run_id}"""
        return f"{cls.PREFIX}:run:result:{run_id}"

    @classmethod
    def run_claim(cls, run_id: str) -> str:
        """hfa:run:claim:{run_id}"""
        return f"{cls.PREFIX}:run:claim:{run_id}"

    @classmethod
    def run_cancel(cls, run_id: str) -> str:
        """hfa:run:cancel:{run_id}"""
        return f"{cls.PREFIX}:run:cancel:{run_id}"

    @classmethod
    def run_payload(cls, run_id: str) -> str:
        """hfa:run:payload:{run_id} — stored for DLQ replay."""
        return f"{cls.PREFIX}:run:payload:{run_id}"

    # ------------------------------------------------------------------
    # Tenant keys
    # ------------------------------------------------------------------

    @classmethod
    def tenant_config(cls, tenant_id: str) -> str:
        """hfa:tenant:{tenant_id}:config"""
        return f"{cls.PREFIX}:tenant:{tenant_id}:config"

    @classmethod
    def tenant_inflight(cls, tenant_id: str) -> str:
        """hfa:tenant:{tenant_id}:inflight — integer counter."""
        return f"{cls.PREFIX}:tenant:{tenant_id}:inflight"

    @classmethod
    def tenant_rate(cls, tenant_id: str) -> str:
        """hfa:tenant:{tenant_id}:rate — sliding-window ZSET."""
        return f"{cls.PREFIX}:tenant:{tenant_id}:rate"

    @classmethod
    def tenant_queue(cls, tenant_id: str) -> str:
        """hfa:tenant:{tenant_id}:queue — per-tenant run ZSET (score=priority+time)."""
        return f"{cls.PREFIX}:tenant:{tenant_id}:queue"

    @classmethod
    def tenant_active_set(cls) -> str:
        """hfa:cp:tenant:active — SET of tenant_ids with non-empty queues."""
        return f"{cls.PREFIX}:cp:tenant:active"

    # ------------------------------------------------------------------
    # Control-plane keys
    # ------------------------------------------------------------------

    @classmethod
    def cp_running(cls) -> str:
        """hfa:cp:running — ZSET of active run_ids."""
        return f"{cls.PREFIX}:cp:running"

    @classmethod
    def cp_leader(cls) -> str:
        """hfa:cp:leader — current leader instance_id."""
        return f"{cls.PREFIX}:cp:leader"

    @classmethod
    def cp_fence(cls) -> str:
        """hfa:cp:fence — monotonic fencing token."""
        return f"{cls.PREFIX}:cp:fence"

    @classmethod
    def cp_dlq_meta(cls, run_id: str) -> str:
        """hfa:cp:dlq:meta:{run_id}"""
        return f"{cls.PREFIX}:cp:dlq:meta:{run_id}"

    # ------------------------------------------------------------------
    # Worker keys
    # ------------------------------------------------------------------

    @classmethod
    def worker(cls, worker_id: str) -> str:
        """hfa:worker:{worker_id}"""
        return f"{cls.PREFIX}:worker:{worker_id}"

    # ------------------------------------------------------------------
    # Worker keys (control-plane registry)
    # ------------------------------------------------------------------

    @classmethod
    def cp_worker(cls, worker_id: str) -> str:
        """hfa:cp:worker:{worker_id} — worker profile HASH."""
        return f"{cls.PREFIX}:cp:worker:{worker_id}"

    @classmethod
    def cp_workers_by_region(cls, region: str) -> str:
        """hfa:cp:workers:by_region:{region} — SET of worker_ids."""
        return f"{cls.PREFIX}:cp:workers:by_region:{region}"

    @classmethod
    def cp_workers_scan_pattern(cls) -> str:
        """hfa:cp:worker:* — glob for KEYS / SCAN over all worker hashes."""
        return f"{cls.PREFIX}:cp:worker:*"

    # ------------------------------------------------------------------
    # Shard ownership keys (control-plane)
    # ------------------------------------------------------------------

    @classmethod
    def cp_shard_owner(cls, shard: int) -> str:
        """hfa:cp:shard:owner:{shard} — single shard owner STRING (TTL'd)."""
        return f"{cls.PREFIX}:cp:shard:owner:{shard}"

    @classmethod
    def cp_shard_owners(cls) -> str:
        """hfa:cp:shard:owners — HASH of all shard→worker_group mappings."""
        return f"{cls.PREFIX}:cp:shard:owners"

    # ------------------------------------------------------------------
    # Observability / graph keys
    # ------------------------------------------------------------------

    @classmethod
    def graph_snapshot(cls, run_id: str) -> str:
        """hfa:graph:snap:{run_id} — serialized run graph snapshot."""
        return f"{cls.PREFIX}:graph:snap:{run_id}"

    @classmethod
    def graph_patch(cls, run_id: str) -> str:
        """hfa:graph:patch:{run_id} — incremental patch LIST."""
        return f"{cls.PREFIX}:graph:patch:{run_id}"

    # ------------------------------------------------------------------
    # Stream keys (used for xadd / xreadgroup)
    # ------------------------------------------------------------------

    @classmethod
    def stream_control(cls) -> str:
        """hfa:stream:control"""
        return f"{cls.PREFIX}:stream:control"

    @classmethod
    def stream_shard(cls, shard: int) -> str:
        """hfa:stream:runs:{shard}"""
        return f"{cls.PREFIX}:stream:runs:{shard}"

    @classmethod
    def stream_results(cls) -> str:
        """hfa:stream:results"""
        return f"{cls.PREFIX}:stream:results"

    @classmethod
    def stream_heartbeat(cls) -> str:
        """hfa:stream:heartbeat"""
        return f"{cls.PREFIX}:stream:heartbeat"

    @classmethod
    def stream_dlq(cls) -> str:
        """hfa:stream:dlq"""
        return f"{cls.PREFIX}:stream:dlq"
