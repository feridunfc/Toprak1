"""Semantic advisory/gate boundary helpers.

Semantic systems may run as advisory sidecars or as authoritative gates.  The
modes are intentionally explicit: advisory mode can degrade, gate mode must fail
closed and expose an observable verdict.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class SemanticVerdict:
    mode: str
    allowed: bool
    reason: str
    observable: bool = True
    details: dict[str, Any] = field(default_factory=dict)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _coerce_verdict(raw: Any, *, mode: str) -> SemanticVerdict:
    if isinstance(raw, SemanticVerdict):
        return raw
    if isinstance(raw, Mapping):
        allowed = bool(raw.get("allowed", raw.get("ok", False)))
        reason = str(raw.get("reason", "semantic_verdict"))
        details = dict(raw.get("details", {}))
        return SemanticVerdict(mode=mode, allowed=allowed, reason=reason, details=details)
    if isinstance(raw, bool):
        return SemanticVerdict(mode=mode, allowed=raw, reason="boolean_semantic_verdict")
    return SemanticVerdict(
        mode=mode,
        allowed=False,
        reason="unrecognized_semantic_verdict",
        details={"raw_type": type(raw).__name__},
    )


async def evaluate_semantic_boundary(
    evaluator: Any,
    payload: Mapping[str, Any],
    *,
    mode: str = "advisory",
    timeout_reason: str = "semantic_unavailable",
) -> SemanticVerdict:
    """
    Evaluate semantic policy in advisory or gate mode.

    In advisory mode, evaluator failures return allowed=True with an observable
    degradation reason.  In gate mode, any error, missing evaluator, or invalid
    verdict returns allowed=False.
    """
    normalized_mode = mode.lower().strip()
    if normalized_mode not in {"advisory", "gate"}:
        raise ValueError("mode must be 'advisory' or 'gate'")

    if evaluator is None:
        return SemanticVerdict(
            mode=normalized_mode,
            allowed=normalized_mode == "advisory",
            reason="semantic_evaluator_missing" if normalized_mode == "gate" else "semantic_advisory_missing",
        )

    try:
        if hasattr(evaluator, "evaluate"):
            raw = evaluator.evaluate(dict(payload), mode=normalized_mode)
        else:
            raw = evaluator(dict(payload), mode=normalized_mode)
        verdict = _coerce_verdict(await _maybe_await(raw), mode=normalized_mode)
    except Exception as exc:
        return SemanticVerdict(
            mode=normalized_mode,
            allowed=normalized_mode == "advisory",
            reason=timeout_reason if normalized_mode == "advisory" else "semantic_gate_fail_closed",
            details={"error": str(exc), "error_type": type(exc).__name__},
        )

    if normalized_mode == "gate" and not verdict.allowed:
        return SemanticVerdict(
            mode="gate",
            allowed=False,
            reason=verdict.reason or "semantic_gate_denied",
            details=verdict.details,
        )
    return verdict
