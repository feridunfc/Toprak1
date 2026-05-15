from __future__ import annotations

import pytest

from hfa_semantic.runtime.semantic_hook import (
    evaluate_advisory_semantics,
    evaluate_gate_semantics,
)


class RaisingEvaluator:
    def evaluate(self, payload, mode="advisory"):
        raise RuntimeError("semantic offline")


class DenyEvaluator:
    def evaluate(self, payload, mode="advisory"):
        return {"allowed": False, "reason": "policy_denied", "details": {"mode": mode}}


@pytest.mark.asyncio
async def test_advisory_semantics_may_fail_open(monkeypatch):
    monkeypatch.setenv("IRON_SEMANTIC_GATE_MODE", "advisory")

    verdict = await evaluate_advisory_semantics(
        RaisingEvaluator(),
        {"run_id": "run-advisory", "tenant_id": "tenant-a"},
    )

    assert verdict.mode == "advisory"
    assert verdict.allowed is True
    assert verdict.replay_visible is True
    assert verdict.audit_visible is True


@pytest.mark.asyncio
async def test_gate_semantics_fail_closed(monkeypatch):
    monkeypatch.setenv("IRON_SEMANTIC_GATE_MODE", "gate")

    verdict = await evaluate_gate_semantics(
        RaisingEvaluator(),
        {"run_id": "run-gate", "tenant_id": "tenant-a"},
    )

    assert verdict.mode == "gate"
    assert verdict.allowed is False
    assert verdict.reason == "semantic_gate_fail_closed"
    assert verdict.replay_visible is True
    assert verdict.audit_visible is True


@pytest.mark.asyncio
async def test_gate_denial_is_replay_visible(monkeypatch):
    monkeypatch.setenv("IRON_SEMANTIC_GATE_MODE", "gate")

    verdict = await evaluate_gate_semantics(
        DenyEvaluator(),
        {"run_id": "run-deny", "tenant_id": "tenant-a"},
    )

    assert verdict.allowed is False
    assert verdict.reason == "policy_denied"
    assert verdict.replay_visible is True
    assert verdict.audit_visible is True


@pytest.mark.asyncio
async def test_legacy_mode_rolls_gate_back_to_advisory(monkeypatch):
    monkeypatch.setenv("IRON_SEMANTIC_GATE_MODE", "legacy")

    verdict = await evaluate_gate_semantics(
        RaisingEvaluator(),
        {"run_id": "run-legacy", "tenant_id": "tenant-a"},
    )

    assert verdict.mode == "advisory"
    assert verdict.allowed is True
