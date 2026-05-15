from __future__ import annotations

from hfa_control.task_recovery import TaskRecoveryManager, recovery_proof_decision


class FakeRedis:
    pass


def test_ambiguous_proof_blocks_auto_resume(monkeypatch):
    monkeypatch.setenv("IRON_V3_PROOF_ENFORCEMENT", "1")
    decision = recovery_proof_decision(gaps_detected=True)
    assert decision.ambiguous is True
    assert decision.auto_resume_allowed is False
    assert decision.requires_manual is True


def test_clean_proof_allows_auto_resume(monkeypatch):
    monkeypatch.setenv("IRON_V3_PROOF_ENFORCEMENT", "1")
    decision = TaskRecoveryManager(FakeRedis()).proof_allows_auto_resume()
    assert decision.ambiguous is False
    assert decision.auto_resume_allowed is True


def test_deterministic_replay_failure_blocks_resume(monkeypatch):
    monkeypatch.setenv("IRON_V3_PROOF_ENFORCEMENT", "1")
    decision = recovery_proof_decision(deterministic_replay_ok=False)
    assert decision.auto_resume_allowed is False
    assert "deterministic_replay_failed" in decision.reason
