"""
tests/core/test_sprint2_epoch_fencing.py
-----------------------------------------
Sprint 2 unit tests for epoch fencing logic.
Tests run against fakeredis — no real Redis required.
"""
from __future__ import annotations

import pytest

from hfa.dag.schema import DagRedisKey, TaskMetaField
from hfa_control.idempotent_completion import IdempotentCompletionGuard, IdempotentCompletionResult
from hfa_control.task_ownership import TaskOwnershipFence, OwnershipFenceToken

pytestmark = pytest.mark.asyncio


# ── OwnershipFence.check_full ─────────────────────────────────────────────────

class TestOwnershipFenceCheckFull:
    def test_all_match_ok(self):
        stored   = OwnershipFenceToken("wA", "sched-1", "3")
        expected = OwnershipFenceToken("wA", "sched-1", "3")
        assert TaskOwnershipFence.check_full(stored, expected).ok is True

    def test_worker_mismatch(self):
        stored   = OwnershipFenceToken("wA", "sched-1", "3")
        expected = OwnershipFenceToken("wB", "sched-1", "3")
        r = TaskOwnershipFence.check_full(stored, expected)
        assert r.ok is False

    def test_scheduler_epoch_mismatch(self):
        stored   = OwnershipFenceToken("wA", "sched-1", "3")
        expected = OwnershipFenceToken("wA", "sched-2", "3")
        r = TaskOwnershipFence.check_full(stored, expected)
        assert r.ok is False
        assert r.status == "scheduler_epoch_mismatch"

    def test_claim_epoch_mismatch(self):
        stored   = OwnershipFenceToken("wA", "sched-1", "3")
        expected = OwnershipFenceToken("wA", "sched-1", "2")
        r = TaskOwnershipFence.check_full(stored, expected)
        assert r.ok is False
        assert r.status == "claim_epoch_mismatch"

    def test_empty_expected_skips_check(self):
        stored   = OwnershipFenceToken("wA", "sched-1", "5")
        expected = OwnershipFenceToken("", "", "")
        assert TaskOwnershipFence.check_full(stored, expected).ok is True

    def test_monotonic_epoch_higher_generation_rejected(self):
        """
        Same worker, higher stored epoch (new claim) must reject old expected epoch.
        This validates the core monotonic epoch property.
        """
        # Worker-A claimed twice: epoch went 1 → 2.
        # Old token from epoch=1 must be rejected.
        stored   = OwnershipFenceToken("wA", "sched-1", "2")
        expected = OwnershipFenceToken("wA", "sched-1", "1")  # stale
        r = TaskOwnershipFence.check_full(stored, expected)
        assert r.ok is False
        assert r.status == "claim_epoch_mismatch"


# ── IdempotentCompletionGuard.classify ────────────────────────────────────────

class TestIdempotentCompletionClassify:
    def test_committed(self):
        r = IdempotentCompletionGuard.classify("committed")
        assert r.ok is True
        assert r.is_duplicate is False
        assert r.is_fence_rejected is False

    def test_already_terminal_is_duplicate(self):
        r = IdempotentCompletionGuard.classify("already_terminal")
        assert r.ok is False
        assert r.is_duplicate is True
        assert r.is_fence_rejected is False

    def test_owner_mismatch_is_fence_rejected(self):
        r = IdempotentCompletionGuard.classify("owner_mismatch")
        assert r.ok is False
        assert r.is_fence_rejected is True

    def test_scheduler_epoch_mismatch_is_fence_rejected(self):
        r = IdempotentCompletionGuard.classify("scheduler_epoch_mismatch")
        assert r.ok is False
        assert r.is_fence_rejected is True

    def test_claim_epoch_mismatch_is_fence_rejected(self):
        r = IdempotentCompletionGuard.classify("claim_epoch_mismatch")
        assert r.ok is False
        assert r.is_fence_rejected is True

    def test_illegal_transition_is_fence_rejected(self):
        r = IdempotentCompletionGuard.classify("illegal_transition")
        assert r.ok is False
        assert r.is_fence_rejected is True
