"""
hfa-control/src/hfa_control/audit.py
IRONCLAD Sprint 17 — Audit logger (control-plane event sink)

Design
------
AuditLogger is a thin async wrapper around SignedLedger that emits
tamper-evident audit entries for critical control-plane operations:

  ADMITTED       — run accepted through admission gates
  REJECTED       — run rejected (reason included)
  SCHEDULED      — run placed on worker
  DRAIN_STARTED  — operator initiated worker drain
  DLQ_REPLAY     — operator replayed a dead-lettered run
  DLQ_DELETED    — operator deleted a DLQ entry
  RESCHEDULE     — operator forced reschedule

Every entry is Ed25519-signed. The chain is append-only and tamper-evident:
any modification to a historical entry breaks the signature of all subsequent
entries.

Fallback behavior
-----------------
If the ledger key is not configured (HFA_LEDGER_KEY_ID env not set), audit
logging silently no-ops. This preserves backward compatibility for environments
that have not yet set up key management.

Failure mode
------------
Audit failures are logged as ERROR but never propagate to callers.
Audit must not break the critical path (admission / scheduling).

Usage
-----
    audit = AuditLogger(redis, key_provider)
    await audit.initialise()

    await audit.admitted(run_id="r", tenant_id="t", agent_type="coder")
    await audit.rejected(run_id="r", tenant_id="t", reason="quota_exceeded")

IRONCLAD rules
--------------
* No print() — logging only.
* Never raise from public methods — log and continue.
* cost_cents: int — no float USD.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Audit event types
# ---------------------------------------------------------------------------


class AuditEvent:
    ADMITTED = "ADMITTED"
    REJECTED = "REJECTED"
    SCHEDULED = "SCHEDULED"
    DRAIN_STARTED = "DRAIN_STARTED"
    DLQ_REPLAY = "DLQ_REPLAY"
    DLQ_DELETED = "DLQ_DELETED"
    RESCHEDULE = "RESCHEDULE"
    AUTH_FAILURE = "AUTH_FAILURE"


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------


class AuditLogger:
    """
    Async audit logger backed by SignedLedger.

    Args:
        redis:        aioredis.Redis client (for RedisLedgerStore).
        key_provider: Ed25519KeyProvider instance. If None, audit is disabled.
    """

    def __init__(self, redis=None, key_provider=None) -> None:
        self._redis = redis
        self._key_provider = key_provider
        self._ledger = None
        self._enabled = key_provider is not None

    async def initialise(self) -> None:
        """
        Set up the SignedLedger with a Redis-backed store.
        Silently disables audit if key provider is missing.
        """
        if not self._enabled:
            logger.info("AuditLogger: disabled (no key provider configured)")
            return

        try:
            from hfa.governance.signed_ledger_v1 import SignedLedger
            from hfa_control.audit_store import RedisLedgerStore

            store = RedisLedgerStore(self._redis)
            self._ledger = SignedLedger(
                key_provider=self._key_provider,
                store=store,
            )
            logger.info(
                "AuditLogger: initialised (key_id=%s)",
                self._key_provider.key_id,
            )
        except Exception as exc:
            logger.error("AuditLogger.initialise failed: %s", exc, exc_info=True)
            self._enabled = False

    # ------------------------------------------------------------------
    # Public audit methods
    # ------------------------------------------------------------------

    async def admitted(
        self,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        priority: int = 5,
        estimated_cost_cents: int = 0,
    ) -> None:
        await self._emit(
            event_type=AuditEvent.ADMITTED,
            run_id=run_id,
            tenant_id=tenant_id,
            data={
                "agent_type": agent_type,
                "priority": priority,
                "estimated_cost_cents": estimated_cost_cents,
            },
        )

    async def rejected(
        self,
        run_id: str,
        tenant_id: str,
        reason: str,
    ) -> None:
        await self._emit(
            event_type=AuditEvent.REJECTED,
            run_id=run_id,
            tenant_id=tenant_id,
            data={"reason": reason},
        )

    async def scheduled(
        self,
        run_id: str,
        tenant_id: str,
        worker_group: str,
        shard: int,
        policy: str,
    ) -> None:
        await self._emit(
            event_type=AuditEvent.SCHEDULED,
            run_id=run_id,
            tenant_id=tenant_id,
            data={
                "worker_group": worker_group,
                "shard": shard,
                "policy": policy,
            },
        )

    async def drain_started(
        self,
        worker_id: str,
        operator: str = "system",
        reason: str = "",
    ) -> None:
        await self._emit(
            event_type=AuditEvent.DRAIN_STARTED,
            run_id=worker_id,  # reuse run_id field for worker_id
            tenant_id="__operator__",
            data={
                "worker_id": worker_id,
                "operator": operator,
                "reason": reason,
            },
        )

    async def dlq_replay(
        self,
        run_id: str,
        tenant_id: str,
        operator: str = "system",
    ) -> None:
        await self._emit(
            event_type=AuditEvent.DLQ_REPLAY,
            run_id=run_id,
            tenant_id=tenant_id,
            data={"operator": operator},
        )

    async def dlq_deleted(
        self,
        run_id: str,
        tenant_id: str,
        operator: str = "system",
    ) -> None:
        await self._emit(
            event_type=AuditEvent.DLQ_DELETED,
            run_id=run_id,
            tenant_id=tenant_id,
            data={"operator": operator},
        )

    async def reschedule(
        self,
        run_id: str,
        tenant_id: str,
        operator: str = "system",
    ) -> None:
        await self._emit(
            event_type=AuditEvent.RESCHEDULE,
            run_id=run_id,
            tenant_id=tenant_id,
            data={"operator": operator},
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _emit(
        self,
        event_type: str,
        run_id: str,
        tenant_id: str,
        data: dict[str, Any],
    ) -> None:
        """
        Append a signed audit entry. Never raises — logs errors and continues.
        """
        if not self._enabled or self._ledger is None:
            return

        payload: dict[str, Any] = {
            "event_type": event_type,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "ts": time.time(),
            **data,
        }

        try:
            await self._ledger.append(
                tenant_id=tenant_id,
                run_id=run_id,
                event_type=event_type,
                payload=payload,
            )
            logger.debug(
                "Audit: %s run=%s tenant=%s",
                event_type,
                run_id,
                tenant_id,
            )
        except Exception as exc:
            logger.error(
                "AuditLogger._emit failed: event=%s run=%s %s",
                event_type,
                run_id,
                exc,
                exc_info=True,
            )
            # Never propagate — audit must not break the critical path


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------


def build_audit_logger(redis) -> AuditLogger:
    """
    Build an AuditLogger from environment variables.

    Returns a no-op AuditLogger if HFA_LEDGER_KEY_ID is not set.
    """
    import os

    key_id = os.environ.get("HFA_LEDGER_KEY_ID", "").strip()

    if not key_id:
        logger.info(
            "AuditLogger: HFA_LEDGER_KEY_ID not set — audit logging disabled. "
            "Set HFA_LEDGER_KEY_ID and HFA_LEDGER_PRIVATE_KEY_B64 to enable."
        )
        return AuditLogger()

    try:
        from hfa.governance.signed_ledger_v1 import Ed25519EnvKeyProvider

        provider = Ed25519EnvKeyProvider()
        return AuditLogger(redis=redis, key_provider=provider)
    except Exception as exc:
        logger.error(
            "AuditLogger: failed to load key provider: %s — audit disabled",
            exc,
        )
        return AuditLogger()
