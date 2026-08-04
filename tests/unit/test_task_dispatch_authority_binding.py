from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hfa.authority import OperationType
from hfa.dag.schema import (
    DagRedisKey,
    DagTaskDispatchInput,
)
from hfa_control.dag_lua import (
    DagLua,
    TaskDispatchCommitResult,
)
from hfa_control.dag_scheduler_dispatch_controller import (
    DagSchedulerDispatchController,
)
from hfa_control.scheduler_reservation_dispatch import (
    ReservationDispatchResult,
    SchedulerReservationDispatcher,
)
from hfa_control.task_dispatch_authority import (
    FEATURE_FLAG,
    WRITER_ID,
    build_task_dispatch_command,
    build_task_dispatch_context,
    normalize_task_dispatch_input,
    parse_task_dispatch_binding_flag,
)
from hfa_control.worker_reservation import (
    WorkerReservationResult,
)


def dispatch(**changes) -> DagTaskDispatchInput:
    values = dict(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_id="worker-1",
        worker_group="group-1",
        agent_type="default",
        shard=2,
        priority=5,
        admitted_at=1000,
        scheduled_at=2000,
        scheduled_zset=(
            DagRedisKey.task_scheduled_zset(
                "tenant-1"
            )
        ),
        running_zset=(
            DagRedisKey.task_running_zset(
                "tenant-1"
            )
        ),
        control_stream="hfa:stream:control",
        shard_stream="hfa:stream:shard:2",
        region="eu",
        policy="LEAST_LOADED",
        payload_json='{"x":1}',
        trace_parent="trace",
        trace_state="state",
        scheduler_epoch="epoch-7",
        attempt=1,
    )
    values.update(changes)
    return DagTaskDispatchInput(**values)


def test_feature_flag_defaults_disabled():
    assert parse_task_dispatch_binding_flag(None) is False
    assert parse_task_dispatch_binding_flag("") is False


@pytest.mark.parametrize(
    "value",
    ["1", " true ", "YES", "on"],
)
def test_feature_flag_accepts_true_values(value):
    assert parse_task_dispatch_binding_flag(value) is True


@pytest.mark.parametrize(
    "value",
    ["0", " false ", "NO", "off"],
)
def test_feature_flag_accepts_false_values(value):
    assert parse_task_dispatch_binding_flag(value) is False


def test_feature_flag_rejects_unknown_value():
    with pytest.raises(ValueError, match=FEATURE_FLAG):
        parse_task_dispatch_binding_flag("maybe")


def test_dispatch_binding_requires_task_admit_binding():
    with pytest.raises(
        ValueError,
        match="requires canonical TASK_ADMIT",
    ):
        DagLua(
            object(),
            canonical_task_admit_binding=False,
            canonical_task_dispatch_binding=True,
        )


def test_writer_identity_and_fence_are_fixed():
    command = build_task_dispatch_command(dispatch())
    context = build_task_dispatch_context(
        command,
        scheduler_epoch="epoch-7",
    )
    assert context.authenticated_writer_id == WRITER_ID
    assert context.allowed_operations == frozenset(
        {OperationType.TASK_DISPATCH}
    )
    assert context.fence_required is True
    assert context.fence_valid is True


def test_same_attempt_has_stable_operation_identity():
    first = build_task_dispatch_command(dispatch())
    changed = build_task_dispatch_command(
        dispatch(
            worker_id="worker-2",
            scheduler_epoch="epoch-8",
            scheduled_at=9000,
        )
    )
    assert first.operation_id == changed.operation_id
    assert (
        first.canonical_command_hash
        != changed.canonical_command_hash
    )


def test_new_attempt_has_new_operation_identity():
    first = build_task_dispatch_command(
        dispatch(attempt=1)
    )
    second = build_task_dispatch_command(
        dispatch(attempt=2)
    )
    assert first.operation_id != second.operation_id


def test_dispatch_command_has_exact_projection_intents():
    command = build_task_dispatch_command(dispatch())
    assert command.intended_previous_state == "ready"
    assert command.intended_next_state == "scheduled"
    assert {
        item["kind"]
        for item in command.requested_projection_intents
    } == {
        "CONTROL_NOTIFICATION",
        "TASK_REQUEST_MESSAGE",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"attempt": 0},
        {"attempt": True},
        {"priority": True},
        {"shard": 1.5},
        {"scheduled_at": -1},
        {"scheduler_epoch": "0"},
        {"worker_id": ""},
    ],
)
def test_dispatch_normalization_fails_closed(changes):
    with pytest.raises(ValueError):
        normalize_task_dispatch_input(
            dispatch(**changes)
        )


@pytest.mark.asyncio
async def test_dispatcher_issues_authoritative_attempt_and_time():
    captured = {}

    class RedisStub:
        async def get(self, _key):
            return "admitted"

        async def hget(self, key, field):
            assert key == DagRedisKey.task_meta(
                "task-1"
            )
            assert field == "requeue_count"
            return "2"

    reservation = AsyncMock()
    reservation.reserve.return_value = (
        WorkerReservationResult(
            ok=True,
            status="reservation_created",
            scheduler_epoch="epoch-1",
        )
    )

    async def writer(**kwargs):
        captured.update(kwargs)
        return TaskDispatchCommitResult(
            committed=True,
            status="committed",
            task_id="task-1",
        )

    dispatcher = SchedulerReservationDispatcher(
        reservation,
        writer,
        redis=RedisStub(),
    )
    result = await dispatcher.reserve_and_dispatch(
        task_id="task-1",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
        dispatch_payload={
            "task_id": "task-1",
            "run_id": "run-1",
            "tenant_id": "tenant-1",
            # Untrusted payload values must be replaced.
            "attempt": 99,
            "scheduled_at": 999999,
        },
        reserved_at_ms=1234,
    )

    assert result.ok is True
    assert captured["dispatch_payload"]["attempt"] == 3
    assert (
        captured["dispatch_payload"]["scheduled_at"]
        == 1234
    )
    reservation.reserve.assert_awaited_once()
    assert (
        reservation.reserve.await_args.kwargs[
            "reserved_at_ms"
        ]
        == 1234
    )


@pytest.mark.asyncio
async def test_durable_projection_failure_keeps_reservation():
    class RedisStub:
        async def get(self, _key):
            return "admitted"

        async def hget(self, _key, _field):
            return None

    reservation = AsyncMock()
    reservation.reserve.return_value = (
        WorkerReservationResult(
            ok=True,
            status="reservation_created",
            scheduler_epoch="epoch-1",
        )
    )

    class ProjectionPending(RuntimeError):
        canonical_commit_durable = True
        status = "canonical_projection_pending"
        detail = "projection unavailable"

    async def writer(**_kwargs):
        raise ProjectionPending()

    dispatcher = SchedulerReservationDispatcher(
        reservation,
        writer,
        redis=RedisStub(),
    )
    result = await dispatcher.reserve_and_dispatch(
        task_id="task-1",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
        dispatch_payload={
            "task_id": "task-1",
            "run_id": "run-1",
            "tenant_id": "tenant-1",
        },
        reserved_at_ms=1234,
    )

    assert result.ok is False
    assert (
        result.status
        == "canonical_projection_pending"
    )
    reservation.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_precommit_failure_releases_reservation():
    class RedisStub:
        async def get(self, _key):
            return "admitted"

        async def hget(self, _key, _field):
            return None

    reservation = AsyncMock()
    reservation.reserve.return_value = (
        WorkerReservationResult(
            ok=True,
            status="reservation_created",
            scheduler_epoch="epoch-1",
        )
    )

    async def writer(**_kwargs):
        raise RuntimeError("authority unavailable")

    dispatcher = SchedulerReservationDispatcher(
        reservation,
        writer,
        redis=RedisStub(),
    )
    result = await dispatcher.reserve_and_dispatch(
        task_id="task-1",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
        dispatch_payload={
            "task_id": "task-1",
            "run_id": "run-1",
            "tenant_id": "tenant-1",
        },
        reserved_at_ms=1234,
    )

    assert result.ok is False
    assert result.status == "dispatch_failed"
    reservation.release.assert_awaited_once_with(
        "worker-1"
    )


@pytest.mark.asyncio
async def test_already_projected_skips_fairness_and_success_hooks():
    rebuilt = dispatch(
        worker_id="",
        worker_group="",
        scheduler_epoch="",
    )

    class ReadyQueue:
        async def list_active_tenants(self):
            return ["tenant-1"]

        async def peek(self, _tenant):
            return "task-1"

        async def rebuild_dispatch_input(
            self,
            *_args,
            **_kwargs,
        ):
            return rebuilt

    fairness = SimpleNamespace(
        pick_next=AsyncMock(return_value="tenant-1"),
        update_on_dispatch=AsyncMock(),
    )
    pacing = SimpleNamespace(
        on_dispatch_success=AsyncMock(),
        on_dispatch_failure=AsyncMock(),
    )
    reservation_dispatcher = SimpleNamespace(
        reserve_and_dispatch=AsyncMock(
            return_value=ReservationDispatchResult(
                ok=True,
                status="already_projected",
                worker_id="worker-1",
                task_id="task-1",
                run_id="run-1",
                tenant_id="tenant-1",
                scheduler_epoch="epoch-1",
                committed_state="scheduled",
                idempotent_replay=True,
            )
        )
    )
    shards = SimpleNamespace(
        shard_for_group=AsyncMock(return_value=2)
    )
    controller = DagSchedulerDispatchController(
        ready_queue=ReadyQueue(),
        tenant_fairness=fairness,
        dispatch_controller=pacing,
        reservation_dispatcher=(
            reservation_dispatcher
        ),
        shards=shards,
    )
    snapshot = SimpleNamespace(
        workers=[
            SimpleNamespace(
                schedulable=True,
                worker_id="worker-1",
                worker_group="group-1",
                capacity=1,
                current_load=0,
                available_slots=1,
                capabilities=[],
            )
        ]
    )

    result = await controller.dispatch_once(
        snapshot=snapshot,
        scheduler_epoch="epoch-1",
    )

    assert result.dispatched is True
    assert result.status == "already_projected"
    fairness.update_on_dispatch.assert_not_awaited()
    pacing.on_dispatch_success.assert_not_awaited()
