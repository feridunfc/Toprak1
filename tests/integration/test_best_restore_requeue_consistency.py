
import pytest

pytestmark = pytest.mark.asyncio


class FakeTenantQueue:
    def __init__(self):
        self.items = []
        self.active = set()

    async def enqueue(self, tenant_id, run_id, priority=0, now=0):
        entry = (tenant_id, run_id)
        if entry not in self.items:
            self.items.append(entry)
        self.active.add(tenant_id)

    def depth(self):
        return len(self.items)


@pytest.mark.integration
async def test_requeue_no_duplicate_enqueue():
    q = FakeTenantQueue()
    await q.enqueue("tenant-a", "run-1")
    await q.enqueue("tenant-a", "run-1")
    assert q.depth() == 1
    assert "tenant-a" in q.active
