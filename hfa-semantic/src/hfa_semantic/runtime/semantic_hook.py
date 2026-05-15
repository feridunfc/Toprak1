"""Semantic advisory/gate boundary helpers.

Sprint 7 constitutional rule:
* advisory semantics may fail open
* gate semantics must fail closed
* policy/safety verdicts must be replay/audit visible

The helpers stay additive and rollback-friendly. Set
``IRON_SEMANTIC_GATE_MODE=legacy`` to retain advisory-only legacy behavior.
"""
from __future__ import annotations

import inspect
import os
from dataclasses import dataclass, field
from typing import Any, Mapping

try:
    from hfa_semantic.validation.safety_verdict import (
        SafetyVerdict,
        advisory_degraded_verdict,
        coerce_safety_verdict,
        fail_closed_verdict,
    )
except Exception:  # pragma: no cover - compatibility when validation package is absent
    SafetyVerdict = None  # type: ignore[assignment]


_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}
_LEGACY_VALUES = {"legacy", "off", "0", "false", "False"}


def semantic_gate_mode() -> str:
    """Return configured semantic mode.

    Values:
    * legacy/advisory: semantic checks are advisory and may degrade
    * gate/strict/v3: semantic checks are authoritative and fail closed
    """

    return os.getenv("IRON_SEMANTIC_GATE_MODE", "legacy").strip().lower() or "legacy"


def is_semantic_gate_enabled() -> bool:
    return semantic_gate_mode() not in _LEGACY_VALUES


@dataclass(frozen=True)
class SemanticVerdict:
    mode: str
    allowed: bool
    reason: str
    observable: bool = True
    replay_visible: bool = True
    audit_visible: bool = True
    details: dict[str, Any] = field(default_factory=dict)

    def to_event_payload(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "allowed": self.allowed,
            "reason": self.reason,
            "observable": self.observable,
            "replay_visible": self.replay_visible,
            "audit_visible": self.audit_visible,
            "details": dict(self.details),
        }


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _payload_identity(payload: Mapping[str, Any]) -> tuple[str, str]:
    return str(payload.get("run_id", "") or ""), str(payload.get("tenant_id", "") or "")


def _coerce_verdict(raw: Any, *, mode: str, payload: Mapping[str, Any] | None = None) -> SemanticVerdict:
    payload = payload or {}
    run_id, tenant_id = _payload_identity(payload)

    if isinstance(raw, SemanticVerdict):
        return raw

    if SafetyVerdict is not None:
        safety = coerce_safety_verdict(raw, mode=mode, run_id=run_id, tenant_id=tenant_id)
        return SemanticVerdict(
            mode=safety.mode,
            allowed=safety.allowed,
            reason=safety.reason,
            observable=safety.audit_visible or safety.replay_visible,
            replay_visible=safety.replay_visible,
            audit_visible=safety.audit_visible,
            details=safety.to_event_payload(),
        )

    if isinstance(raw, Mapping):
        allowed = bool(raw.get("allowed", raw.get("ok", False)))
        reason = str(raw.get("reason", "semantic_verdict"))
        details = dict(raw.get("details", {}))
        return SemanticVerdict(mode=mode, allowed=allowed, reason=reason, details=details)
    if isinstance(raw, bool):
        return SemanticVerdict(mode=mode, allowed=raw, reason="boolean_semantic_verdict")
    return SemanticVerdict(
        mode=mode,
        allowed=mode == "advisory",
        reason="unrecognized_semantic_verdict",
        details={"raw_type": type(raw).__name__},
    )


def _safe_verdict_from_error(
    *,
    mode: str,
    payload: Mapping[str, Any],
    reason: str,
    exc: BaseException | None = None,
) -> SemanticVerdict:
    run_id, tenant_id = _payload_identity(payload)
    if SafetyVerdict is not None and mode == "gate":
        safety = fail_closed_verdict(reason=reason, run_id=run_id, tenant_id=tenant_id, error=exc)
        return SemanticVerdict(
            mode="gate",
            allowed=False,
            reason=safety.reason,
            replay_visible=safety.replay_visible,
            audit_visible=safety.audit_visible,
            details=safety.to_event_payload(),
        )
    if SafetyVerdict is not None:
        safety = advisory_degraded_verdict(reason=reason, run_id=run_id, tenant_id=tenant_id, error=exc)
        return SemanticVerdict(
            mode="advisory",
            allowed=True,
            reason=safety.reason,
            replay_visible=safety.replay_visible,
            audit_visible=safety.audit_visible,
            details=safety.to_event_payload(),
        )

    details: dict[str, Any] = {}
    if exc is not None:
        details = {"error": str(exc), "error_type": type(exc).__name__}
    return SemanticVerdict(
        mode=mode,
        allowed=mode == "advisory",
        reason=reason,
        details=details,
    )


async def evaluate_semantic_boundary(
    evaluator: Any,
    payload: Mapping[str, Any],
    *,
    mode: str = "advisory",
    timeout_reason: str = "semantic_unavailable",
) -> SemanticVerdict:
    """Evaluate semantic policy in advisory or gate mode.

    In advisory mode, evaluator failures return allowed=True with an observable
    degradation reason. In gate mode, any error, missing evaluator, or invalid
    verdict returns allowed=False.
    """

    normalized_mode = mode.lower().strip()
    if normalized_mode not in {"advisory", "gate"}:
        raise ValueError("mode must be 'advisory' or 'gate'")

    if evaluator is None:
        reason = "semantic_evaluator_missing" if normalized_mode == "gate" else "semantic_advisory_missing"
        return _safe_verdict_from_error(mode=normalized_mode, payload=payload, reason=reason)

    try:
        if hasattr(evaluator, "evaluate"):
            raw = evaluator.evaluate(dict(payload), mode=normalized_mode)
        else:
            raw = evaluator(dict(payload), mode=normalized_mode)
        verdict = _coerce_verdict(await _maybe_await(raw), mode=normalized_mode, payload=payload)
    except Exception as exc:
        return _safe_verdict_from_error(
            mode=normalized_mode,
            payload=payload,
            reason=timeout_reason if normalized_mode == "advisory" else "semantic_gate_fail_closed",
            exc=exc,
        )

    if normalized_mode == "gate" and not verdict.allowed:
        return SemanticVerdict(
            mode="gate",
            allowed=False,
            reason=verdict.reason or "semantic_gate_denied",
            observable=True,
            replay_visible=True,
            audit_visible=True,
            details=verdict.details,
        )
    return verdict


async def evaluate_advisory_semantics(evaluator: Any, payload: Mapping[str, Any]) -> SemanticVerdict:
    return await evaluate_semantic_boundary(evaluator, payload, mode="advisory")


async def evaluate_gate_semantics(evaluator: Any, payload: Mapping[str, Any]) -> SemanticVerdict:
    """Evaluate the v3 semantic gate.

    If ``IRON_SEMANTIC_GATE_MODE=legacy`` this intentionally behaves as
    advisory for rollback. Any non-legacy gate mode fails closed.
    """

    if not is_semantic_gate_enabled():
        return await evaluate_semantic_boundary(evaluator, payload, mode="advisory")
    return await evaluate_semantic_boundary(evaluator, payload, mode="gate")
