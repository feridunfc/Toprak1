"""
tests/core/test_worker_backward_compat.py

Sprint 8 — Rewritten to test canonical ExecutionResult (hfa_worker.models).

The old test verified the pre-Sprint-1 ExecutionResult API (output_text, usage).
That API is gone. This test now verifies the canonical contract that all
executors must satisfy per integration_contract.md.
"""

from hfa_worker.models import ExecutionResult


def test_execution_result_done():
    result = ExecutionResult(
        status="done",
        payload={"output": "hello"},
        cost_cents=42,
        tokens_used=100,
    )
    assert result.status == "done"
    assert result.is_success is True
    assert result.is_terminal_failure is False
    assert result.cost_cents == 42
    assert result.tokens_used == 100
    assert result.payload == {"output": "hello"}
    assert result.error is None


def test_execution_result_failed():
    result = ExecutionResult(
        status="failed",
        payload={},
        error="something went wrong",
        cost_cents=0,
        tokens_used=3,
    )
    assert result.status == "failed"
    assert result.is_success is False
    assert result.is_terminal_failure is True
    assert result.error == "something went wrong"
    assert result.tokens_used == 3


def test_execution_result_defaults():
    result = ExecutionResult(status="done")
    assert result.payload == {}
    assert result.error is None
    assert result.cost_cents == 0
    assert result.tokens_used == 0


def test_execution_result_status_validation():
    import pytest
    with pytest.raises(AssertionError):
        ExecutionResult(status="running")  # must be "done" or "failed"
