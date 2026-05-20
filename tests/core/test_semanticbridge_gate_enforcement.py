from hfa_agents.integration.semantic_bridge import (
    CANONICAL_AUTHORITY_WRITES_ALLOWED,
    ADVISORY_ONLY_SURFACE,
    SemanticBridge,
    evaluate_semantic_gate_decision,
)


def test_semanticbridge_surface_remains_advisory_non_authoritative():
    assert ADVISORY_ONLY_SURFACE is True
    assert CANONICAL_AUTHORITY_WRITES_ALLOWED is False


def test_semantic_gate_decision_fails_closed_on_missing_verdict():
    decision = evaluate_semantic_gate_decision(None)

    assert decision.rejected is True
    assert decision.allowed is False
    assert decision.reason == "semantic_gate_missing_verdict"
    assert decision.replay_visible is True
    assert decision.audit_visible is True


def test_semantic_gate_decision_rejects_low_confidence_allowed_verdict():
    decision = evaluate_semantic_gate_decision(
        {
            "mode": "gate",
            "allowed": True,
            "reason": "semantic_ok",
            "confidence": 0.10,
        }
    )

    assert decision.rejected is True
    assert decision.reason == "confidence_below_threshold:0.100"


def test_semantic_gate_decision_accepts_high_confidence_allowed_verdict():
    decision = evaluate_semantic_gate_decision(
        {
            "mode": "gate",
            "allowed": True,
            "reason": "semantic_ok",
            "confidence": 0.91,
        }
    )

    assert decision.allowed is True
    assert decision.reason == "semantic_ok"


async def _boom(*args, **kwargs):
    raise RuntimeError("boom")


def test_semanticbridge_gate_without_hook_fails_closed(monkeypatch):
    import hfa_agents.integration.semantic_bridge as bridge_module

    monkeypatch.setattr(bridge_module, "evaluate_gate_semantics", None)

    import asyncio

    bridge = SemanticBridge(semantic_pipeline=None)
    verdict = asyncio.run(bridge.evaluate_gate({"event_id": "evt-1"}))

    assert verdict["allowed"] is False
    assert verdict["reason"] == "semantic_hook_unavailable"
    assert verdict["replay_visible"] is True
    assert verdict["audit_visible"] is True


def test_semanticbridge_gate_exception_fails_closed(monkeypatch):
    import hfa_agents.integration.semantic_bridge as bridge_module

    monkeypatch.setattr(bridge_module, "evaluate_gate_semantics", _boom)

    import asyncio

    bridge = SemanticBridge(semantic_pipeline=None)
    verdict = asyncio.run(bridge.evaluate_gate({"event_id": "evt-1"}))

    assert verdict["allowed"] is False
    assert verdict["reason"] == "semantic_gate_exception"
    assert verdict["replay_visible"] is True
    assert verdict["audit_visible"] is True
