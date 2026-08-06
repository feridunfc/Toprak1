from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os

import pytest
import pytest_asyncio

try:
    import redis.asyncio as redis_asyncio
except ImportError as exc:  # mandatory integration dependency
    raise RuntimeError("redis.asyncio is required for Sprint 84.4 integration") from exc

from hfa.authority import RedisCanonicalAuthorityStore, evaluate_authority_commit
from hfa.config.keys import RedisKey
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationInput,
    AdmissionResourceReservationManager,
    RESERVATION_STATE_FINALIZED,
)
from hfa_control.run_create_authority import (
    RUN_CREATE_DUPLICATE_STATUS,
    RUN_CREATE_PROJECTED_STATUS,
    RunCreateAuthorityBinding,
    RunCreateAuthorityInput,
    RunCreateAuthorityError,
    RunCreateProjectionManager,
    build_run_create_command,
    build_run_create_context,
    run_create_operation_id,
)


REDIS_URL = os.getenv("HFA_TEST_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/15"


@dataclass
class Request:
    run_id: str
    tenant_id: str
    agent_type: str = "research"
    priority: int = 5
    payload: dict = None
    estimated_cost_cents: int = 100
    preferred_region: str = "eu-west-1"
    preferred_placement: str = "LEAST_LOADED"

    def __post_init__(self):
        if self.payload is None:
            self.payload = {"prompt": "hello"}


@pytest_asyncio.fixture
async def real_redis():
    client = redis_asyncio.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.fail(f"mandatory real Redis unavailable at {REDIS_URL}: {exc}")
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


def components(redis, *, store=None):
    resource = AdmissionResourceReservationManager(redis, clock_ms=lambda: 1234)
    authority_store = store or RedisCanonicalAuthorityStore(redis)
    projection = RunCreateProjectionManager(redis)
    binding = RunCreateAuthorityBinding(
        redis=redis,
        resource_manager=resource,
        control_stream=RedisKey.stream_control(),
        store=authority_store,
        projection_manager=projection,
    )
    return binding, resource, authority_store, projection


async def event_rows(redis, run_id):
    rows = await redis.xrange(RedisKey.stream_control(), min="-", max="+")
    return [
        (entry_id, fields)
        for entry_id, fields in rows
        if fields.get("event_type") == "RunAdmitted" and fields.get("run_id") == run_id
    ]


async def manually_commit(redis, resource, store, request, *, finalize=False):
    operation_id = run_create_operation_id(request.run_id)
    reservation = AdmissionResourceReservationInput(
        operation_id=operation_id,
        run_id=request.run_id,
        tenant_id=request.tenant_id,
        estimated_cost_cents=request.estimated_cost_cents,
    )
    await resource.initialise()
    reserved = await resource.reserve_once(
        reservation,
        concurrent_run_limit=None,
        budget_limit_cents=None,
        tenant_inflight_limit=10,
    )
    assert reserved.status == "reserved"
    receipt = await resource.get_receipt(reservation)
    assert receipt is not None
    value = RunCreateAuthorityInput(
        run_id=request.run_id,
        tenant_id=request.tenant_id,
        agent_type=request.agent_type,
        priority=request.priority,
        payload=request.payload,
        estimated_cost_cents=request.estimated_cost_cents,
        preferred_region=request.preferred_region,
        preferred_placement=request.preferred_placement,
        created_at_ms=receipt.created_at_ms,
        control_stream=RedisKey.stream_control(),
    )
    command = build_run_create_command(value)
    evaluation = evaluate_authority_commit(
        context=build_run_create_context(command),
        command=command,
        current_revision=0,
        current_state=None,
        receipt_probe=None,
        committed_at_ms=receipt.created_at_ms,
        correlation_id=None,
    )
    assert evaluation.commit_plan is not None
    await store.initialise()
    persisted = await store.commit(evaluation.commit_plan)
    assert persisted.status.value == "COMMITTED"
    if finalize:
        finalized = await resource.finalize_once(reservation)
        assert finalized.state == RESERVATION_STATE_FINALIZED
    return reservation, evaluation.commit_plan


@pytest.mark.asyncio
async def test_first_and_exact_duplicate_emit_one_event(real_redis):
    binding, resource, store, projection = components(real_redis)
    request = Request("tenant-a:run-first", "tenant-a")
    first = await binding.admit(request, tenant_inflight_limit=10)
    second = await binding.admit(request, tenant_inflight_limit=10)
    assert first.status == RUN_CREATE_PROJECTED_STATUS
    assert second.status == RUN_CREATE_DUPLICATE_STATUS
    assert first.stream_entry_id == second.stream_entry_id
    assert await real_redis.get(RedisKey.run_state(request.run_id)) == "admitted"
    rows = await event_rows(real_redis, request.run_id)
    assert len(rows) == 1
    assert rows[0][1]["tenant_id"] == request.tenant_id
    assert rows[0][1]["payload"] == '{"prompt":"hello"}'
    snapshot = await store.get_aggregate_snapshot(
        build_run_create_command(
            RunCreateAuthorityInput(
                run_id=request.run_id,
                tenant_id=request.tenant_id,
                agent_type=request.agent_type,
                priority=request.priority,
                payload=request.payload,
                estimated_cost_cents=request.estimated_cost_cents,
                preferred_region=request.preferred_region,
                preferred_placement=request.preferred_placement,
                created_at_ms=1234,
                control_stream=RedisKey.stream_control(),
            )
        ).aggregate_identity
    )
    assert snapshot is not None
    assert snapshot.revision == 1
    receipt = await resource.get_receipt(
        AdmissionResourceReservationInput(
            operation_id=run_create_operation_id(request.run_id),
            run_id=request.run_id,
            tenant_id=request.tenant_id,
            estimated_cost_cents=request.estimated_cost_cents,
        )
    )
    assert receipt is not None and receipt.state == RESERVATION_STATE_FINALIZED
    assert await real_redis.ttl(resource.concurrent_run_key(request.tenant_id)) == -1
    assert await real_redis.ttl(resource.budget_reserved_key(request.tenant_id)) == -1
    assert await real_redis.ttl(RedisKey.tenant_inflight(request.tenant_id)) == -1
    assert await real_redis.ttl(
        projection.receipt_key(run_create_operation_id(request.run_id))
    ) == -1


@pytest.mark.asyncio
async def test_concurrent_identical_calls_commit_and_emit_once(real_redis):
    binding, _resource, _store, _projection = components(real_redis)
    request = Request("tenant-a:run-concurrent", "tenant-a")
    results = await asyncio.gather(
        binding.admit(request, tenant_inflight_limit=10),
        binding.admit(request, tenant_inflight_limit=10),
    )
    assert sorted(result.status for result in results) == sorted(
        [RUN_CREATE_PROJECTED_STATUS, RUN_CREATE_DUPLICATE_STATUS]
    )
    assert len(await event_rows(real_redis, request.run_id)) == 1


@pytest.mark.asyncio
async def test_changed_cost_conflicts_before_second_mutation(real_redis):
    binding, resource, _store, _projection = components(real_redis)
    request = Request("tenant-a:run-cost", "tenant-a", estimated_cost_cents=100)
    await binding.admit(request, tenant_inflight_limit=10)
    with pytest.raises(RunCreateAuthorityError):
        await binding.admit(
            Request(request.run_id, request.tenant_id, estimated_cost_cents=101),
            tenant_inflight_limit=10,
        )
    snapshot = await resource.get_resource_snapshot(request.tenant_id)
    assert snapshot == {
        "concurrent_runs": 1,
        "budget_reserved_cents": 100,
        "tenant_inflight": 1,
    }
    assert len(await event_rows(real_redis, request.run_id)) == 1


@pytest.mark.asyncio
async def test_changed_payload_conflicts_at_canonical_receipt(real_redis):
    binding, resource, _store, _projection = components(real_redis)
    request = Request("tenant-a:run-payload", "tenant-a", payload={"v": 1})
    await binding.admit(request, tenant_inflight_limit=10)
    with pytest.raises(RunCreateAuthorityError):
        await binding.admit(
            Request(request.run_id, request.tenant_id, payload={"v": 2}),
            tenant_inflight_limit=10,
        )
    assert (await resource.get_resource_snapshot(request.tenant_id))["concurrent_runs"] == 1
    assert len(await event_rows(real_redis, request.run_id)) == 1


@pytest.mark.asyncio
async def test_tenant_inflight_limit_rejects_without_canonical_or_event(real_redis):
    binding, resource, store, _projection = components(real_redis)
    request = Request("tenant-a:run-limit", "tenant-a")
    with pytest.raises(RunCreateAuthorityError) as caught:
        await binding.admit(request, tenant_inflight_limit=0)
    assert caught.value.resource_status == "tenant_inflight_exceeded"
    assert await event_rows(real_redis, request.run_id) == []
    assert await real_redis.get(RedisKey.run_state(request.run_id)) is None
    identity = build_run_create_command(
        RunCreateAuthorityInput(
            run_id=request.run_id,
            tenant_id=request.tenant_id,
            agent_type=request.agent_type,
            priority=request.priority,
            payload=request.payload,
            estimated_cost_cents=request.estimated_cost_cents,
            preferred_region=request.preferred_region,
            preferred_placement=request.preferred_placement,
            created_at_ms=1234,
            control_stream=RedisKey.stream_control(),
        )
    ).aggregate_identity
    assert await store.get_aggregate_snapshot(identity) is None
    assert await resource.get_resource_snapshot(request.tenant_id) == {
        "concurrent_runs": 0,
        "budget_reserved_cents": 0,
        "tenant_inflight": 0,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ttl", "wrong_type", "overflow"])
async def test_counter_conflicts_fail_before_partial_mutation(real_redis, mode):
    binding, resource, _store, _projection = components(real_redis)
    request = Request(f"tenant-a:run-{mode}", "tenant-a")
    key = resource.concurrent_run_key(request.tenant_id)
    if mode == "ttl":
        await real_redis.set(key, "0", ex=60)
    elif mode == "wrong_type":
        await real_redis.rpush(key, "0")
    else:
        await real_redis.set(key, str(2**53 - 1))
    with pytest.raises(RunCreateAuthorityError):
        await binding.admit(request, tenant_inflight_limit=10)
    assert await event_rows(real_redis, request.run_id) == []
    assert await real_redis.get(RedisKey.run_state(request.run_id)) is None


@pytest.mark.asyncio
async def test_legacy_state_without_canonical_evidence_releases_new_reservation(real_redis):
    binding, resource, _store, _projection = components(real_redis)
    request = Request("tenant-a:run-legacy", "tenant-a")
    await real_redis.set(RedisKey.run_state(request.run_id), "admitted")
    with pytest.raises(RunCreateAuthorityError, match="legacy_RUN_CREATE"):
        await binding.admit(request, tenant_inflight_limit=10)
    assert await resource.get_resource_snapshot(request.tenant_id) == {
        "concurrent_runs": 0,
        "budget_reserved_cents": 0,
        "tenant_inflight": 0,
    }


@pytest.mark.asyncio
async def test_crash_a_after_reserve_before_commit_retries_to_one_event(real_redis):
    binding, resource, store, _projection = components(real_redis)
    request = Request("tenant-a:run-crash-a", "tenant-a")
    operation = AdmissionResourceReservationInput(
        operation_id=run_create_operation_id(request.run_id),
        run_id=request.run_id,
        tenant_id=request.tenant_id,
        estimated_cost_cents=request.estimated_cost_cents,
    )
    await resource.reserve_once(
        operation,
        concurrent_run_limit=None,
        budget_limit_cents=None,
        tenant_inflight_limit=10,
    )
    result = await binding.admit(request, tenant_inflight_limit=10)
    assert result.status == RUN_CREATE_PROJECTED_STATUS
    assert len(await event_rows(real_redis, request.run_id)) == 1
    assert (await resource.get_resource_snapshot(request.tenant_id))["concurrent_runs"] == 1


@pytest.mark.asyncio
async def test_crash_b_after_commit_before_finalize_retries_without_release(real_redis):
    binding, resource, store, _projection = components(real_redis)
    request = Request("tenant-a:run-crash-b", "tenant-a")
    await manually_commit(real_redis, resource, store, request, finalize=False)
    result = await binding.admit(request, tenant_inflight_limit=10)
    assert result.status == RUN_CREATE_PROJECTED_STATUS
    assert len(await event_rows(real_redis, request.run_id)) == 1
    assert (await resource.get_resource_snapshot(request.tenant_id))["concurrent_runs"] == 1


@pytest.mark.asyncio
async def test_crash_c_after_finalize_before_projection_retries_once(real_redis):
    binding, resource, store, _projection = components(real_redis)
    request = Request("tenant-a:run-crash-c", "tenant-a")
    await manually_commit(real_redis, resource, store, request, finalize=True)
    result = await binding.admit(request, tenant_inflight_limit=10)
    assert result.status == RUN_CREATE_PROJECTED_STATUS
    assert len(await event_rows(real_redis, request.run_id)) == 1


@pytest.mark.asyncio
async def test_crash_d_after_projection_before_response_is_duplicate_noop(real_redis):
    binding, _resource, _store, _projection = components(real_redis)
    request = Request("tenant-a:run-crash-d", "tenant-a")
    first = await binding.admit(request, tenant_inflight_limit=10)
    retry = await binding.admit(request, tenant_inflight_limit=10)
    assert first.status == RUN_CREATE_PROJECTED_STATUS
    assert retry.status == RUN_CREATE_DUPLICATE_STATUS
    assert len(await event_rows(real_redis, request.run_id)) == 1


class AmbiguousStore:
    def __init__(self, inner):
        self.inner = inner
        self.raise_once = True

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def commit(self, plan):
        result = await self.inner.commit(plan)
        if self.raise_once:
            self.raise_once = False
            raise RuntimeError("simulated lost commit response")
        return result


@pytest.mark.asyncio
async def test_crash_e_ambiguous_commit_rereads_durable_proof_and_never_releases(real_redis):
    inner = RedisCanonicalAuthorityStore(real_redis)
    ambiguous = AmbiguousStore(inner)
    binding, resource, _store, _projection = components(real_redis, store=ambiguous)
    request = Request("tenant-a:run-crash-e", "tenant-a")
    result = await binding.admit(request, tenant_inflight_limit=10)
    assert result.status == RUN_CREATE_PROJECTED_STATUS
    assert (await resource.get_resource_snapshot(request.tenant_id))["concurrent_runs"] == 1
    assert len(await event_rows(real_redis, request.run_id)) == 1

@pytest.mark.asyncio
async def test_partial_legacy_state_after_canonical_commit_fails_closed_without_release(
    real_redis,
):
    binding, resource, store, _projection = components(real_redis)
    request = Request("tenant-a:run-partial-state", "tenant-a")
    await manually_commit(real_redis, resource, store, request, finalize=True)
    await real_redis.set(RedisKey.run_state(request.run_id), "admitted")

    with pytest.raises(RunCreateAuthorityError) as caught:
        await binding.admit(request, tenant_inflight_limit=10)

    assert caught.value.canonical_commit_durable is True
    assert len(await event_rows(real_redis, request.run_id)) == 0
    assert (await resource.get_resource_snapshot(request.tenant_id)) == {
        "concurrent_runs": 1,
        "budget_reserved_cents": request.estimated_cost_cents,
        "tenant_inflight": 1,
    }


@pytest.mark.asyncio
async def test_partial_legacy_event_after_canonical_commit_fails_closed_without_duplicate(
    real_redis,
):
    binding, resource, store, _projection = components(real_redis)
    request = Request("tenant-a:run-partial-event", "tenant-a")
    await manually_commit(real_redis, resource, store, request, finalize=True)
    await real_redis.xadd(
        RedisKey.stream_control(),
        {
            "event_type": "RunAdmitted",
            "run_id": request.run_id,
            "tenant_id": request.tenant_id,
        },
    )

    with pytest.raises(RunCreateAuthorityError) as caught:
        await binding.admit(request, tenant_inflight_limit=10)

    assert caught.value.canonical_commit_durable is True
    assert len(await event_rows(real_redis, request.run_id)) == 1
    assert await real_redis.get(RedisKey.run_state(request.run_id)) is None
    assert (await resource.get_resource_snapshot(request.tenant_id))["concurrent_runs"] == 1


@pytest.mark.asyncio
async def test_corrupt_projection_receipt_fails_closed_without_state_or_event_mutation(
    real_redis,
):
    binding, resource, store, projection = components(real_redis)
    request = Request("tenant-a:run-corrupt-projection", "tenant-a")
    await manually_commit(real_redis, resource, store, request, finalize=True)
    await real_redis.hset(
        projection.receipt_key(run_create_operation_id(request.run_id)),
        mapping={"operation_id": run_create_operation_id(request.run_id)},
    )

    with pytest.raises(RunCreateAuthorityError) as caught:
        await binding.admit(request, tenant_inflight_limit=10)

    assert caught.value.canonical_commit_durable is True
    assert await real_redis.get(RedisKey.run_state(request.run_id)) is None
    assert await event_rows(real_redis, request.run_id) == []
    assert (await resource.get_resource_snapshot(request.tenant_id))["concurrent_runs"] == 1


@pytest.mark.asyncio
async def test_projection_receipt_wrong_type_fails_closed(real_redis):
    binding, resource, store, projection = components(real_redis)
    request = Request("tenant-a:run-projection-type", "tenant-a")
    await manually_commit(real_redis, resource, store, request, finalize=True)
    await real_redis.set(
        projection.receipt_key(run_create_operation_id(request.run_id)),
        "wrong-type",
    )

    with pytest.raises(RunCreateAuthorityError):
        await binding.admit(request, tenant_inflight_limit=10)

    assert await real_redis.get(RedisKey.run_state(request.run_id)) is None
    assert await event_rows(real_redis, request.run_id) == []
