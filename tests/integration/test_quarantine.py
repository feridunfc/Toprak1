from __future__ import annotations

import pytest

from hfa_control.scheduler_loop import SchedulerLoop


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str):
        self.values[key] = value
        return True

    async def exists(self, key: str):
        return 1 if key in self.values else 0

    async def get(self, key: str):
        return self.values.get(key)


class Controller:
    def __init__(self) -> None:
        self.redis = FakeRedis()


@pytest.mark.asyncio
async def test_quarantine_marks_scheduler_projection(monkeypatch):
    monkeypatch.setenv("IRON_V3_PROOF_ENFORCEMENT", "1")
    loop = SchedulerLoop(dispatch_controller=Controller(), config=None)

    await loop._quarantine_run("run-q", "proof_ambiguous")

    assert await loop._is_run_quarantined("run-q") is True
    assert loop._dispatch_controller.redis.values["hfa:quarantine:run-q"] == "proof_ambiguous"


@pytest.mark.asyncio
async def test_scheduler_blocks_quarantined_dispatch(monkeypatch):
    monkeypatch.setenv("IRON_V3_PROOF_ENFORCEMENT", "1")
    loop = SchedulerLoop(dispatch_controller=Controller(), config=None)
    await loop._quarantine_run("run-q2", "manual_required")

    result = await loop.commit_dispatch("run-q2")

    assert result is False
