from __future__ import annotations

import pytest

from hfa_agents.integration.semantic_bridge import SemanticBridge
from hfa_worker.scheduler_semantic_hook import evaluate_scheduler_semantic_gate


class AllowEvaluator:
    def evaluate(self, payload, mode="advisory"):
        return {
            "allowed": True,
            "reason": "ok",
            "run_id": payload.get("run_id", ""),
            "tenant_id": payload.get("tenant_id", ""),
        }


class RaisingEvaluator:
    def evaluate(self, payload, mode="advisory"):
        raise RuntimeError("offline")


@pytest.mark.asyncio
async def test_semantic_bridge_gate_is_separate_from_advisory(monkeypatch):
    monkeypatch.setenv("IRON_SEMANTIC_GATE_MODE", "gate")
    bridge = SemanticBridge(semantic_pipeline=None, gate_evaluator=AllowEvaluator())

    enriched = await bridge.enrich_event({"event_id": "e1", "run_id": "run-1"})
    verdict = await bridge.evaluate_gate({"event_id": "e1", "run_id": "run-1"})

    assert enriched is not None
    assert enriched["semantic_mode"] == "advisory"
    assert verdict.mode == "gate"
    assert verdict.allowed is True


@pytest.mark.asyncio
async def test_semantic_bridge_gate_fails_closed_when_unavailable(monkeypatch):
    monkeypatch.setenv("IRON_SEMANTIC_GATE_MODE", "gate")
    bridge = SemanticBridge(semantic_pipeline=None, gate_evaluator=RaisingEvaluator())

    verdict = await bridge.evaluate_gate({"event_id": "e2", "run_id": "run-2"})

    assert verdict.mode == "gate"
    assert verdict.allowed is False
    assert verdict.audit_visible is True
    assert verdict.replay_visible is True


@pytest.mark.asyncio
async def test_scheduler_semantic_gate_helper_fails_closed(monkeypatch):
    monkeypatch.setenv("IRON_SEMANTIC_GATE_MODE", "gate")

    verdict = await evaluate_scheduler_semantic_gate(
        run_id="run-3",
        tenant_id="tenant-a",
        agent_type="coder",
        evaluator=RaisingEvaluator(),
    )

    assert verdict.mode == "gate"
    assert verdict.allowed is False
    assert verdict.audit_visible is True
    assert verdict.replay_visible is True
