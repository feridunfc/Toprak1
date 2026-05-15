from __future__ import annotations

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.reconciliation_manager import (
    ReconciliationManager,
    evaluate_authority_proof,
)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.zsets: dict[str, set[str]] = {}

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str):
        self.values[key] = value
        return True

    async def zcard(self, key: str):
        return len(self.zsets.get(key, set()))


@pytest.mark.asyncio
async def test_reconcile_blocks_critical_drift_when_proof_enforced(monkeypatch):
    monkeypatch.setenv("IRON_V3_PROOF_ENFORCEMENT", "1")
    redis = FakeRedis()
    tenant_id = "tenant-a"
    redis.values[DagRedisKey.tenant_inflight(tenant_id)] = "3"
    redis.zsets[DagRedisKey.task_running_zset(tenant_id)] = {"task-1"}

    report = await ReconciliationManager(redis).reconcile_tenant(tenant_id)

    assert report.ambiguous is True
    assert report.blocked is True
    assert report.requires_manual is True
    assert report.corrected is False
    assert redis.values[DagRedisKey.tenant_inflight(tenant_id)] == "3"


@pytest.mark.asyncio
async def test_reconcile_legacy_corrects_when_flag_disabled(monkeypatch):
    monkeypatch.delenv("IRON_V3_PROOF_ENFORCEMENT", raising=False)
    redis = FakeRedis()
    tenant_id = "tenant-b"
    redis.values[DagRedisKey.tenant_inflight(tenant_id)] = "0"
    redis.zsets[DagRedisKey.task_running_zset(tenant_id)] = {"task-1", "task-2"}

    report = await ReconciliationManager(redis).reconcile_tenant(tenant_id)

    assert report.corrected is True
    assert report.ambiguous is False
    assert redis.values[DagRedisKey.tenant_inflight(tenant_id)] == "2"


def test_gaps_and_duplicates_imply_ambiguous_authority():
    proof = evaluate_authority_proof(gaps_detected=True, duplicates_detected=True)
    assert proof.ambiguous is True
    assert proof.blocks_auto_resume is True
    assert proof.requires_manual is True
