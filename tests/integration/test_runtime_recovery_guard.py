from __future__ import annotations

import pytest

from hfa_worker.runtime.worker_runtime import WorkerRuntime


class FakeRedis:
    def __init__(self) -> None:
        self.values = {"hfa:quarantine:run-worker": "manual_required"}

    async def exists(self, key: str):
        return 1 if key in self.values else 0

    async def get(self, key: str):
        return self.values.get(key)


@pytest.mark.asyncio
async def test_worker_observes_quarantine_before_execution(monkeypatch):
    monkeypatch.setenv("IRON_V3_PROOF_ENFORCEMENT", "1")
    runtime = WorkerRuntime(event_store=None, enabled=False)

    assert await runtime.is_quarantined(FakeRedis(), "run-worker") is True
