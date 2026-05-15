from __future__ import annotations

import os
from dataclasses import dataclass

from hfa.dag.schema import DagRedisKey

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


def is_proof_enforcement_enabled() -> bool:
    """Return True when Sprint 6 proof/quarantine enforcement is active."""
    return os.getenv("IRON_V3_PROOF_ENFORCEMENT", "0") not in _FALSE_VALUES


@dataclass(frozen=True)
class ReconciliationReport:
    tenant_id: str
    expected_inflight: int
    actual_inflight: int
    corrected: bool
    ambiguous: bool = False
    blocked: bool = False
    requires_manual: bool = False
    reason: str = ""


@dataclass(frozen=True)
class AuthorityProof:
    """Deterministic proof verdict for runtime/replay reconciliation.

    This object is intentionally small and local to Sprint 6.  It gives callers a
    stable way to distinguish a correctable projection mismatch from ambiguous
    authority, where automatic correction/resume must not continue.
    """

    replay_clean: bool
    deterministic_replay_ok: bool
    gaps_detected: bool = False
    duplicates_detected: bool = False
    critical_drift: bool = False
    ambiguous: bool = False
    blocks_auto_resume: bool = False
    requires_manual: bool = False
    reason: str = ""


def evaluate_authority_proof(
    *,
    replay_clean: bool = True,
    deterministic_replay_ok: bool = True,
    gaps_detected: bool = False,
    duplicates_detected: bool = False,
    critical_drift: bool = False,
) -> AuthorityProof:
    """Classify replay/runtime evidence without trusting Redis on ambiguity."""
    reasons: list[str] = []
    if gaps_detected:
        reasons.append("gaps_detected")
    if duplicates_detected:
        reasons.append("duplicates_detected")
    if not replay_clean:
        reasons.append("replay_not_clean")
    if not deterministic_replay_ok:
        reasons.append("deterministic_replay_failed")
    if critical_drift:
        reasons.append("critical_drift")

    ambiguous = bool(reasons)
    return AuthorityProof(
        replay_clean=replay_clean,
        deterministic_replay_ok=deterministic_replay_ok,
        gaps_detected=gaps_detected,
        duplicates_detected=duplicates_detected,
        critical_drift=critical_drift,
        ambiguous=ambiguous,
        blocks_auto_resume=ambiguous,
        requires_manual=ambiguous,
        reason=";".join(reasons),
    )


class ReconciliationManager:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def actual_inflight(self, tenant_id: str) -> int:
        return int(await self._redis.zcard(DagRedisKey.task_running_zset(tenant_id)))

    async def expected_inflight(self, tenant_id: str) -> int:
        raw = await self._redis.get(DagRedisKey.tenant_inflight(tenant_id))
        return int(raw) if raw is not None else 0

    async def reconcile_tenant(self, tenant_id: str) -> ReconciliationReport:
        expected = await self.expected_inflight(tenant_id)
        actual = await self.actual_inflight(tenant_id)

        drifted = expected != actual
        if drifted and is_proof_enforcement_enabled():
            # Sprint 6: a drift between runtime projection and actual running set
            # is ambiguous authority.  Do not silently trust Redis or auto-correct;
            # force the manual/proof path instead.
            return ReconciliationReport(
                tenant_id=tenant_id,
                expected_inflight=expected,
                actual_inflight=actual,
                corrected=False,
                ambiguous=True,
                blocked=True,
                requires_manual=True,
                reason="critical_drift_requires_manual_path",
            )

        corrected = drifted
        if corrected:
            await self._redis.set(DagRedisKey.tenant_inflight(tenant_id), str(actual))

        return ReconciliationReport(
            tenant_id=tenant_id,
            expected_inflight=expected,
            actual_inflight=actual,
            corrected=corrected,
        )

    async def reconcile_many(self, tenant_ids: list[str]) -> list[ReconciliationReport]:
        return [await self.reconcile_tenant(tenant_id) for tenant_id in tenant_ids]
