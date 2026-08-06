from __future__ import annotations

import asyncio
import os
import uuid

import pytest
import pytest_asyncio
try:
    import redis.asyncio as redis_asyncio
except ImportError as exc:  # pragma: no cover - mandatory dependency guard
    raise RuntimeError(
        "real Redis integration requires the redis.asyncio client"
    ) from exc

from hfa.config.keys import RedisKey  # noqa: E402
from hfa.governance.admission_resource_reservation import (  # noqa: E402
    AdmissionResourceReservationInput,
    AdmissionResourceReservationManager,
    RELEASED_RECEIPT_TTL_SECONDS,
    RESERVATION_STATUS_ALREADY_FINALIZED,
    RESERVATION_STATUS_ALREADY_RELEASED,
    RESERVATION_STATUS_ALREADY_RESERVED,
    RESERVATION_STATUS_BUDGET_EXCEEDED,
    RESERVATION_STATUS_CONFLICT,
    RESERVATION_STATUS_FINALIZED,
    RESERVATION_STATUS_INFLIGHT_EXCEEDED,
    RESERVATION_STATUS_QUOTA_EXCEEDED,
    RESERVATION_STATUS_RELEASED,
    RESERVATION_STATUS_RESERVED,
    RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
    RESERVATION_STATUS_STATE_CONFLICT,
)
from hfa_control.tenant_registry import TenantRegistry  # noqa: E402

MAX_SAFE_INTEGER = 2**53 - 1


@pytest_asyncio.fixture
async def real_redis():
    url = os.getenv(
        "HFA_TEST_REDIS_URL",
        os.getenv("REDIS_URL", "redis://127.0.0.1:6379/15"),
    )
    client = redis_asyncio.from_url(url, decode_responses=False)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.fail(f"real Redis service is mandatory: {exc}")
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


def item(*, amount=100, suffix=None, tenant="tenant-1", run_id="run-tenant-1"):
    suffix = suffix or uuid.uuid4().hex.ljust(64, "0")[:64]
    return AdmissionResourceReservationInput(
        operation_id=f"run-create:v1:{suffix}",
        run_id=run_id,
        tenant_id=tenant,
        estimated_cost_cents=amount,
    )


async def reserve(
    manager,
    value,
    *,
    now_ms=1000,
    concurrent=10,
    budget=10_000,
    inflight=10,
):
    return await manager.reserve_once(
        value,
        concurrent_run_limit=concurrent,
        budget_limit_cents=budget,
        tenant_inflight_limit=inflight,
        now_ms=now_ms,
    )


async def raw_resource_state(redis, manager, value):
    keys = [
        manager.concurrent_run_key(value.tenant_id),
        manager.budget_reserved_key(value.tenant_id),
        RedisKey.tenant_inflight(value.tenant_id),
    ]
    types = [await redis.type(key) for key in keys]
    values = []
    for key, redis_type in zip(keys, types):
        if redis_type in {b"none", "none"}:
            values.append(None)
        elif redis_type in {b"string", "string"}:
            values.append(await redis.get(key))
        elif redis_type in {b"hash", "hash"}:
            values.append(sorted((await redis.hgetall(key)).items()))
        else:
            values.append(f"<{redis_type!r}>")
    return {
        "keys": keys,
        "values": values,
        "ttls": [await redis.ttl(key) for key in keys],
        "types": types,
        "receipt_exists": await redis.exists(
            manager.reservation_receipt_key(value.operation_id)
        ),
    }


@pytest.mark.asyncio
async def test_first_reserve_and_active_lifetime_invariants(real_redis):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(suffix="a" * 64)
    result = await reserve(manager, value)
    assert result.status == RESERVATION_STATUS_RESERVED
    assert await manager.get_resource_snapshot(value.tenant_id) == {
        "concurrent_runs": 1,
        "budget_reserved_cents": 100,
        "tenant_inflight": 1,
    }
    receipt_key = manager.reservation_receipt_key(value.operation_id)
    keys = [
        receipt_key,
        manager.concurrent_run_key(value.tenant_id),
        manager.budget_reserved_key(value.tenant_id),
        RedisKey.tenant_inflight(value.tenant_id),
    ]
    assert [await real_redis.ttl(key) for key in keys] == [-1, -1, -1, -1]
    assert await TenantRegistry(real_redis).get_inflight(value.tenant_id) == 1


@pytest.mark.asyncio
async def test_exact_retry_finalize_and_released_ttl(real_redis):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(suffix="b" * 64)
    assert (await reserve(manager, value)).status == RESERVATION_STATUS_RESERVED
    before = await manager.get_resource_snapshot(value.tenant_id)
    assert (await reserve(manager, value, now_ms=1001)).status == RESERVATION_STATUS_ALREADY_RESERVED
    assert await manager.get_resource_snapshot(value.tenant_id) == before

    assert (await manager.finalize_once(value, now_ms=1002)).status == RESERVATION_STATUS_FINALIZED
    receipt_key = manager.reservation_receipt_key(value.operation_id)
    assert await real_redis.ttl(receipt_key) == -1
    assert (await manager.finalize_once(value, now_ms=1003)).status == RESERVATION_STATUS_ALREADY_FINALIZED
    assert (await manager.release_once(value, now_ms=1004)).status == RESERVATION_STATUS_STATE_CONFLICT

    releasable = item(suffix="c" * 64, tenant="tenant-2", run_id="run-tenant-2")
    assert (await reserve(manager, releasable)).status == RESERVATION_STATUS_RESERVED
    assert (await manager.release_once(releasable, now_ms=2000)).status == RESERVATION_STATUS_RELEASED
    released_key = manager.reservation_receipt_key(releasable.operation_id)
    released_ttl = await real_redis.ttl(released_key)
    assert 0 < released_ttl <= RELEASED_RECEIPT_TTL_SECONDS
    assert (await manager.release_once(releasable, now_ms=2001)).status == RESERVATION_STATUS_ALREADY_RELEASED


@pytest.mark.asyncio
async def test_limit_and_corrupt_counter_rejections_have_zero_partial_mutation(real_redis):
    manager = AdmissionResourceReservationManager(real_redis)
    cases = [
        (item(suffix="d" * 64), 0, 1000, 10, RESERVATION_STATUS_QUOTA_EXCEEDED),
        (item(amount=101, suffix="e" * 64), 10, 100, 10, RESERVATION_STATUS_BUDGET_EXCEEDED),
        (item(suffix="f" * 64), 10, 1000, 0, RESERVATION_STATUS_INFLIGHT_EXCEEDED),
    ]
    for value, concurrent, budget, inflight, expected in cases:
        result = await reserve(
            manager,
            value,
            concurrent=concurrent,
            budget=budget,
            inflight=inflight,
        )
        assert result.status == expected
        assert await real_redis.exists(
            manager.reservation_receipt_key(value.operation_id)
        ) == 0
        assert await manager.get_resource_snapshot(value.tenant_id) == {
            "concurrent_runs": 0,
            "budget_reserved_cents": 0,
            "tenant_inflight": 0,
        }

    corrupt = item(suffix="1" * 64, tenant="tenant-corrupt", run_id="run-corrupt")
    corrupt_key = manager.concurrent_run_key(corrupt.tenant_id)
    await real_redis.set(corrupt_key, "bad")
    before = await raw_resource_state(real_redis, manager, corrupt)
    result = await reserve(manager, corrupt)
    after = await raw_resource_state(real_redis, manager, corrupt)
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert before == after


@pytest.mark.asyncio
async def test_concurrent_identical_calls_increment_once(real_redis):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(suffix="2" * 64)
    results = await asyncio.gather(
        *[reserve(manager, value, now_ms=3000 + index) for index in range(20)]
    )
    assert sum(result.status == RESERVATION_STATUS_RESERVED for result in results) == 1
    assert sum(result.status == RESERVATION_STATUS_ALREADY_RESERVED for result in results) == 19
    assert await manager.get_resource_snapshot(value.tenant_id) == {
        "concurrent_runs": 1,
        "budget_reserved_cents": 100,
        "tenant_inflight": 1,
    }


@pytest.mark.asyncio
async def test_concurrent_changed_proof_has_one_winner_and_conflicts(real_redis):
    manager = AdmissionResourceReservationManager(real_redis)
    operation_id = f"run-create:v1:{'3' * 64}"
    original = item(suffix="3" * 64, amount=100)
    changed = AdmissionResourceReservationInput(
        operation_id=operation_id,
        run_id=original.run_id,
        tenant_id=original.tenant_id,
        estimated_cost_cents=101,
    )
    results = await asyncio.gather(
        reserve(manager, original, now_ms=4000),
        reserve(manager, changed, now_ms=4001),
    )
    assert sum(result.status == RESERVATION_STATUS_RESERVED for result in results) == 1
    assert sum(result.status == RESERVATION_STATUS_CONFLICT for result in results) == 1
    snapshot = await manager.get_resource_snapshot(original.tenant_id)
    assert snapshot["concurrent_runs"] == 1
    assert snapshot["tenant_inflight"] == 1
    assert snapshot["budget_reserved_cents"] in {100, 101}


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["missing_concurrent", "undersized_budget", "missing_inflight"])
async def test_release_resource_drift_fails_closed(real_redis, drift):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(
        suffix={
            "missing_concurrent": "4" * 64,
            "undersized_budget": "5" * 64,
            "missing_inflight": "6" * 64,
        }[drift]
    )
    assert (await reserve(manager, value)).status == RESERVATION_STATUS_RESERVED

    if drift == "missing_concurrent":
        await real_redis.delete(manager.concurrent_run_key(value.tenant_id))
    elif drift == "undersized_budget":
        await real_redis.set(manager.budget_reserved_key(value.tenant_id), "99")
    else:
        await real_redis.delete(RedisKey.tenant_inflight(value.tenant_id))

    before = await raw_resource_state(real_redis, manager, value)
    result = await manager.release_once(value, now_ms=5000)
    after = await raw_resource_state(real_redis, manager, value)
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert before == after
    receipt = await manager.get_receipt(value)
    assert receipt is not None and receipt.state == "RESERVED"


@pytest.mark.asyncio
async def test_restart_reuses_receipt_and_changed_proof_conflicts(real_redis):
    first = AdmissionResourceReservationManager(real_redis)
    value = item(suffix="7" * 64)
    assert (await reserve(first, value)).status == RESERVATION_STATUS_RESERVED

    restarted = AdmissionResourceReservationManager(real_redis)
    assert (await reserve(restarted, value, now_ms=6000)).status == RESERVATION_STATUS_ALREADY_RESERVED
    changed = AdmissionResourceReservationInput(
        operation_id=value.operation_id,
        run_id="run-changed",
        tenant_id=value.tenant_id,
        estimated_cost_cents=value.estimated_cost_cents,
    )
    assert (await reserve(restarted, changed, now_ms=6001)).status == RESERVATION_STATUS_CONFLICT
    assert await restarted.get_resource_snapshot(value.tenant_id) == {
        "concurrent_runs": 1,
        "budget_reserved_cents": 100,
        "tenant_inflight": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("counter_index", [0, 1, 2])
async def test_first_reserve_rejects_existing_expiring_counter_without_normalization(
    real_redis, counter_index
):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(suffix=format(counter_index + 8, "x") * 64)
    keys = [
        manager.concurrent_run_key(value.tenant_id),
        manager.budget_reserved_key(value.tenant_id),
        RedisKey.tenant_inflight(value.tenant_id),
    ]
    await real_redis.set(keys[counter_index], "7", ex=120)
    before = await raw_resource_state(real_redis, manager, value)
    result = await reserve(manager, value, now_ms=7000)
    after = await raw_resource_state(real_redis, manager, value)
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert before["values"] == after["values"]
    assert before["types"] == after["types"]
    assert after["receipt_exists"] == 0
    assert 0 < after["ttls"][counter_index] <= before["ttls"][counter_index]
    for index in range(3):
        if index != counter_index:
            assert after["values"][index] is None
            assert after["ttls"][index] == -2


@pytest.mark.asyncio
@pytest.mark.parametrize("counter_index", [0, 1, 2])
async def test_first_reserve_rejects_wrong_redis_type_without_partial_mutation(
    real_redis, counter_index
):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(suffix=chr(ord("b") + counter_index) * 64)
    keys = [
        manager.concurrent_run_key(value.tenant_id),
        manager.budget_reserved_key(value.tenant_id),
        RedisKey.tenant_inflight(value.tenant_id),
    ]
    await real_redis.hset(keys[counter_index], mapping={"bad": "type"})
    before = await raw_resource_state(real_redis, manager, value)
    result = await reserve(manager, value, now_ms=8000)
    after = await raw_resource_state(real_redis, manager, value)
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert before == after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "counter_index,counter_value,cost",
    [
        (0, MAX_SAFE_INTEGER, 0),
        (2, MAX_SAFE_INTEGER, 0),
        (1, MAX_SAFE_INTEGER, 1),
        (1, MAX_SAFE_INTEGER - 1, 2),
    ],
)
async def test_safe_integer_addition_overflow_fails_closed(
    real_redis, counter_index, counter_value, cost
):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(amount=cost, suffix=uuid.uuid4().hex.ljust(64, "0")[:64])
    keys = [
        manager.concurrent_run_key(value.tenant_id),
        manager.budget_reserved_key(value.tenant_id),
        RedisKey.tenant_inflight(value.tenant_id),
    ]
    await real_redis.set(keys[counter_index], str(counter_value))
    before = await raw_resource_state(real_redis, manager, value)
    result = await reserve(
        manager,
        value,
        concurrent=None,
        budget=None,
        inflight=None,
        now_ms=9000,
    )
    after = await raw_resource_state(real_redis, manager, value)
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert before == after


@pytest.mark.asyncio
async def test_safe_integer_exact_boundaries_remain_valid(real_redis):
    manager = AdmissionResourceReservationManager(real_redis)
    value = item(amount=1, suffix="f" * 64)
    keys = [
        manager.concurrent_run_key(value.tenant_id),
        manager.budget_reserved_key(value.tenant_id),
        RedisKey.tenant_inflight(value.tenant_id),
    ]
    for key in keys:
        await real_redis.set(key, str(MAX_SAFE_INTEGER - 1))
    result = await reserve(
        manager,
        value,
        concurrent=None,
        budget=None,
        inflight=None,
        now_ms=10000,
    )
    assert result.status == RESERVATION_STATUS_RESERVED
    assert [int(await real_redis.get(key)) for key in keys] == [MAX_SAFE_INTEGER] * 3
    assert [await real_redis.ttl(key) for key in keys] == [-1, -1, -1]
