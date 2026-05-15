"""Replay/audit-visible semantic safety verdicts for IRONCLAD v3.

The semantic layer has two distinct modes:
* advisory: may degrade/fail open and must remain non-authoritative
* gate: must fail closed and must emit an audit/replay-visible verdict

This module is deliberately small and dependency-free so runtime, worker, and
agent integration code can share one verdict shape without importing heavy
semantic pipeline internals.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class SafetyVerdict:
    """Normalized semantic policy/safety verdict.

    `replay_visible` and `audit_visible` intentionally default to True. Sprint 7
    requires gate decisions to be visible to replay/audit surfaces. Advisory
    decisions are also observable, but they are not authoritative.
    """

    mode: str
    allowed: bool
    reason: str
    run_id: str = ""
    tenant_id: str = ""
    policy_id: str = "semantic.default"
    replay_visible: bool = True
    audit_visible: bool = True
    timestamp_ms: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.timestamp_ms:
            object.__setattr__(self, "timestamp_ms", int(time.time() * 1000))

    def to_event_payload(self) -> dict[str, Any]:
        """Return a compact payload suitable for event/audit logs."""

        return {
            "mode": self.mode,
            "allowed": self.allowed,
            "reason": self.reason,
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "policy_id": self.policy_id,
            "replay_visible": self.replay_visible,
            "audit_visible": self.audit_visible,
            "timestamp_ms": self.timestamp_ms,
            "details": dict(self.details),
        }


def coerce_safety_verdict(
    raw: Any,
    *,
    mode: str,
    run_id: str = "",
    tenant_id: str = "",
    default_reason: str = "semantic_verdict",
) -> SafetyVerdict:
    """Coerce common evaluator outputs into a SafetyVerdict.

    Unknown values are denied in gate mode and allowed in advisory mode only
    when the caller explicitly selected advisory.
    """

    normalized_mode = mode.lower().strip()
    if isinstance(raw, SafetyVerdict):
        return raw

    if isinstance(raw, Mapping):
        allowed = bool(raw.get("allowed", raw.get("ok", False)))
        reason = str(raw.get("reason", default_reason))
        return SafetyVerdict(
            mode=normalized_mode,
            allowed=allowed,
            reason=reason,
            run_id=str(raw.get("run_id", run_id) or ""),
            tenant_id=str(raw.get("tenant_id", tenant_id) or ""),
            policy_id=str(raw.get("policy_id", "semantic.default") or "semantic.default"),
            replay_visible=bool(raw.get("replay_visible", True)),
            audit_visible=bool(raw.get("audit_visible", True)),
            details=dict(raw.get("details", {})),
        )

    if isinstance(raw, bool):
        return SafetyVerdict(
            mode=normalized_mode,
            allowed=raw,
            reason="boolean_semantic_verdict",
            run_id=run_id,
            tenant_id=tenant_id,
        )

    return SafetyVerdict(
        mode=normalized_mode,
        allowed=normalized_mode == "advisory",
        reason="unrecognized_semantic_verdict",
        run_id=run_id,
        tenant_id=tenant_id,
        details={"raw_type": type(raw).__name__},
    )


def fail_closed_verdict(
    *,
    reason: str,
    run_id: str = "",
    tenant_id: str = "",
    error: BaseException | None = None,
) -> SafetyVerdict:
    details: dict[str, Any] = {}
    if error is not None:
        details["error"] = str(error)
        details["error_type"] = type(error).__name__
    return SafetyVerdict(
        mode="gate",
        allowed=False,
        reason=reason,
        run_id=run_id,
        tenant_id=tenant_id,
        details=details,
    )


def advisory_degraded_verdict(
    *,
    reason: str,
    run_id: str = "",
    tenant_id: str = "",
    error: BaseException | None = None,
) -> SafetyVerdict:
    details: dict[str, Any] = {}
    if error is not None:
        details["error"] = str(error)
        details["error_type"] = type(error).__name__
    return SafetyVerdict(
        mode="advisory",
        allowed=True,
        reason=reason,
        run_id=run_id,
        tenant_id=tenant_id,
        details=details,
    )
