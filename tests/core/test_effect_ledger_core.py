
import pytest
import pytest_asyncio
from fakeredis import FakeAsyncRedis

from hfa_control.effect_ledger import EffectLedger, EffectReceipt


@pytest_asyncio.fixture
async def redis_client():
    client = FakeAsyncRedis(decode_responses=True)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        close = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result


@pytest_asyncio.fixture
async def ledger(redis_client):
    return EffectLedger(redis_client)


@pytest.mark.asyncio
async def test_first_strike_wins(ledger: EffectLedger):
    receipt = await ledger.acquire_effect(
        run_id="run-1",
        token="dispatch_attempt_1",
        effect_type="dispatch",
        owner_id="scheduler-A",
    )
    assert receipt.accepted is True
    assert receipt.duplicate is False
    assert receipt.owner_id == "scheduler-A"
    assert receipt.committed_state == "pending"


@pytest.mark.asyncio
async def test_duplicate_storm_is_blocked(ledger: EffectLedger):
    first = await ledger.acquire_effect(
        run_id="run-2",
        token="payment_50_usd",
        effect_type="charge",
        owner_id="worker-x",
    )
    assert first.accepted is True

    duplicate = await ledger.acquire_effect(
        run_id="run-2",
        token="payment_50_usd",
        effect_type="charge",
        owner_id="worker-x",
    )
    assert duplicate.accepted is False
    assert duplicate.duplicate is True
    assert duplicate.owner_id == "worker-x"
    assert duplicate.effect_type == "charge"


@pytest.mark.asyncio
async def test_zombie_worker_fencing(ledger: EffectLedger):
    winner = await ledger.acquire_effect(
        run_id="run-3",
        token="task_123_complete",
        effect_type="complete",
        owner_id="worker-A",
    )
    assert winner.accepted is True
    assert winner.owner_id == "worker-A"

    zombie = await ledger.acquire_effect(
        run_id="run-3",
        token="task_123_complete",
        effect_type="complete",
        owner_id="worker-B",
    )
    assert zombie.accepted is False
    assert zombie.duplicate is True
    assert zombie.owner_id == "worker-A"


@pytest.mark.asyncio
async def test_independent_tokens_do_not_collide(ledger: EffectLedger):
    receipt1 = await ledger.acquire_effect("run-4", "token-A", "dispatch", "owner-1")
    receipt2 = await ledger.acquire_effect("run-4", "token-B", "dispatch", "owner-1")
    assert receipt1.accepted is True
    assert receipt2.accepted is True


@pytest.mark.asyncio
async def test_receipt_roundtrip_can_be_read_back(ledger: EffectLedger):
    created = await ledger.acquire_effect(
        run_id="run-5",
        token="comp-task-9",
        effect_type="complete",
        owner_id="worker-z",
    )
    fetched = await ledger.get_receipt(
        run_id="run-5",
        token="comp-task-9",
        effect_type="complete",
    )
    assert isinstance(fetched, EffectReceipt)
    assert fetched.to_json() == created.to_json()
