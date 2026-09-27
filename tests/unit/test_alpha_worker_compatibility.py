"""Readiness and actual dispatch must agree before any reservation is made."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hfa_control.dag_scheduler_dispatch_controller import DagSchedulerDispatchController
from hfa_control.models import ControlPlaneConfig, WorkerStatus
from hfa_control.product_profile import (
    ALPHA_CANONICAL_COMPOSITION_CAPABILITY as MARKER,
    PRODUCT_EXECUTOR_CAPABILITIES,
    ProductMode,
    alpha_worker_is_compatible,
    validate_control_product_profile,
)
from hfa_control.scheduler import build_production_scheduler
from hfa_control.scheduler_snapshot import SchedulerSnapshotBuilder
from hfa_control.service import ControlPlaneService
from hfa_control.tenant_fairness import TenantFairnessTracker


VALID = [MARKER, "product:single-task-v1", "run-finalization:v1", "executor:deterministic"]
OLD = [value for value in VALID if value != MARKER]
FUTURE = "product:single-task-canonical-v2"
INCOMPATIBLE = [
    pytest.param(OLD, id="old-alpha"),
    *[pytest.param([*VALID[:-1], capability], id=capability)
      for capability in ("executor:external", "executor:cognitive", "executor:configured")],
    pytest.param([FUTURE, *OLD], id="unknown-version"),
    pytest.param([*VALID, FUTURE], id="conflicting-versions"),
    pytest.param([v for v in VALID if v != "run-finalization:v1"], id="no-finalization"),
    pytest.param([v for v in VALID if v != "product:single-task-v1"], id="no-product"),
    pytest.param([v for v in VALID if not v.startswith("executor:")], id="no-executor"),
    pytest.param([*VALID[:-1], "executor:spoofed"], id="unknown-executor"),
    pytest.param([*VALID, "executor:external"], id="two-executors"),
    pytest.param([*VALID, 1], id="non-text-capability"),
    pytest.param(dict.fromkeys(VALID, True), id="dict-is-not-capability-list"),
    pytest.param(",".join(VALID), id="string-is-not-capability-list"),
    pytest.param(None, id="missing-evidence"),
    pytest.param(123, id="malformed-evidence"),
]


def worker(worker_id="w1", capabilities=None, **overrides):
    values = dict(
        worker_id=worker_id, worker_group="group-alpha", region="test",
        capabilities=list(VALID) if capabilities is None else capabilities,
        agent_types=[], capacity=10, inflight=0, available_slots=10,
        schedulable=True, status=WorkerStatus.HEALTHY, last_seen=1.0,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def alpha_config():
    return ControlPlaneConfig(
        instance_id="cp-e2", product_mode="SINGLE_TASK_ALPHA",
        strict_cas_mode=True, tenant_identity_boundary="TRUSTED_GATEWAY_HEADER",
    )


def readiness_service(workers):
    service = object.__new__(ControlPlaneService)
    service._product_profile = validate_control_product_profile(
        product_mode="SINGLE_TASK_ALPHA", strict_cas_mode=True,
        tenant_identity_boundary="TRUSTED_GATEWAY_HEADER",
        canonical_task_admit_binding=True, canonical_run_create_binding=True,
        canonical_task_dispatch_binding=True, canonical_task_requeue_binding=True,
        single_task_submission_surface=True,
    )
    service._redis = SimpleNamespace(ping=AsyncMock(return_value=True), get=AsyncMock(return_value="cp-e2"))
    service._registry = SimpleNamespace(list_schedulable_workers=AsyncMock(return_value=workers))
    service._leader = SimpleNamespace(is_leader=True)
    service._scheduler = SimpleNamespace(running=True)
    service._sched_started = True
    service._config = alpha_config()
    return service


def dispatch_controller(mode="SINGLE_TASK_ALPHA", required=()):
    rebuilt = SimpleNamespace(
        task_id="task-1", run_id="run-1", tenant_id="tenant-a",
        required_capabilities=required, dispatch_payload={},
    )
    ready = SimpleNamespace(
        list_active_tenants=AsyncMock(return_value=["tenant-a"]),
        peek=AsyncMock(return_value="task-1"),
        rebuild_dispatch_input=AsyncMock(return_value=rebuilt),
    )
    dispatcher = SimpleNamespace(reserve_and_dispatch=AsyncMock(
        return_value=SimpleNamespace(ok=True, status="committed"),
    ))
    controller = DagSchedulerDispatchController(
        ready_queue=ready, tenant_fairness=TenantFairnessTracker(),
        dispatch_controller=SimpleNamespace(), reservation_dispatcher=dispatcher,
        shards=SimpleNamespace(shard_for_group=AsyncMock(return_value=0)),
        product_mode=mode,
    )
    return controller, dispatcher


@pytest.mark.parametrize("capabilities", INCOMPATIBLE)
@pytest.mark.asyncio
async def test_incompatible_evidence_blocks_readiness_capacity_and_dispatch(capabilities):
    bad = worker()
    bad.capabilities = capabilities
    assert not alpha_worker_is_compatible(capabilities)
    result = await readiness_service([bad]).get_product_readiness()
    assert result["ready"] is False
    assert result["compatible_worker_count"] == 0

    registry = SimpleNamespace(list_all_workers=AsyncMock(return_value=[bad]))
    builder = SchedulerSnapshotBuilder(object(), registry, None, None, alpha_config())
    snapshot = await builder.build_capacity_snapshot()
    assert snapshot.total_available_slots == 0
    assert snapshot.dispatch_allowed is False
    assert snapshot.workers[0].blocked_reason == "product_profile_incompatible"

    # Deliberately bypass the snapshot builder: dispatch must enforce it too.
    controller, dispatcher = dispatch_controller()
    dispatched = await controller.dispatch_once(
        snapshot=SimpleNamespace(workers=(bad,)), scheduler_epoch="1",
    )
    assert dispatched.dispatched is False
    assert dispatched.reason == "product_profile_incompatible"
    dispatcher.reserve_and_dispatch.assert_not_called()


@pytest.mark.parametrize("executor", sorted(PRODUCT_EXECUTOR_CAPABILITIES))
def test_first_release_accepts_only_deterministic_executor(executor):
    assert alpha_worker_is_compatible([*VALID[:-1], executor]) is (executor == "executor:deterministic")


@pytest.mark.asyncio
async def test_mixed_pool_never_assigns_to_legacy_worker_even_if_less_loaded():
    legacy = worker("a-legacy", OLD, capacity=100)
    canonical = worker("z-canonical", inflight=2, available_slots=8)
    service = readiness_service([legacy, canonical])
    assert (await service.get_product_readiness())["compatible_worker_count"] == 1

    controller, dispatcher = dispatch_controller()
    result = await controller.dispatch_once(
        snapshot=SimpleNamespace(workers=(legacy, canonical)), scheduler_epoch="3",
    )
    assert result.dispatched is True
    assert result.worker_id == "z-canonical"
    assert dispatcher.reserve_and_dispatch.await_args.kwargs["worker_id"] == "z-canonical"
    assert dispatcher.reserve_and_dispatch.await_count == 1


@pytest.mark.asyncio
async def test_task_capability_constraints_still_apply_to_compatible_worker():
    controller, dispatcher = dispatch_controller(required=["gpu"])
    result = await controller.dispatch_once(
        snapshot=SimpleNamespace(workers=(worker(),)), scheduler_epoch="1",
    )
    assert result.dispatched is False
    assert result.reason == "no_compatible_workers"
    dispatcher.reserve_and_dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_agent_types_cannot_supply_the_missing_composition_marker():
    bad = worker(capabilities=OLD, agent_types=[MARKER])
    registry = SimpleNamespace(list_all_workers=AsyncMock(return_value=[bad]))
    snapshot = await SchedulerSnapshotBuilder(
        object(), registry, None, None, alpha_config(),
    ).build_capacity_snapshot()
    assert snapshot.workers[0].schedulable is False
    controller, dispatcher = dispatch_controller()
    assert not await controller.dispatch_once(snapshot=snapshot, scheduler_epoch="1")
    dispatcher.reserve_and_dispatch.assert_not_called()


@pytest.mark.parametrize("changes", [
    {"status": WorkerStatus.DRAINING},
    {"status": WorkerStatus.DEAD},
    {"capacity": 0},
    {"inflight": 10},
])
@pytest.mark.asyncio
async def test_composition_marker_does_not_override_health_or_capacity(changes):
    registry = SimpleNamespace(list_all_workers=AsyncMock(return_value=[worker(**changes)]))
    snapshot = await SchedulerSnapshotBuilder(
        object(), registry, None, None, alpha_config(),
    ).build_capacity_snapshot()
    assert snapshot.dispatch_allowed is False
    assert snapshot.total_available_slots == 0


@pytest.mark.asyncio
async def test_internal_scheduler_preserves_preexisting_worker_selection():
    controller, dispatcher = dispatch_controller(mode="RUNTIME_INTERNAL")
    result = await controller.dispatch_once(
        snapshot=SimpleNamespace(workers=(worker(capabilities=["base"]),)),
        scheduler_epoch="1",
    )
    assert result.dispatched is True
    dispatcher.reserve_and_dispatch.assert_awaited_once()


def test_production_builder_passes_alpha_mode_to_dispatch_and_snapshot(monkeypatch):
    monkeypatch.setenv("HFA_CANONICAL_TASK_ADMIT_BINDING", "true")
    monkeypatch.setenv("HFA_CANONICAL_TASK_DISPATCH_BINDING", "true")
    scheduler = build_production_scheduler(
        redis=object(), config=alpha_config(), registry=object(), shards=object(),
    )
    assert scheduler.composition.dispatch_controller._product_mode is ProductMode.SINGLE_TASK_ALPHA
    assert scheduler.composition.scheduler_loop._snapshot_builder._product_mode is ProductMode.SINGLE_TASK_ALPHA


def test_unknown_dispatch_profile_is_rejected():
    with pytest.raises(ValueError, match="unsupported product_mode"):
        dispatch_controller(mode="TYPO_ALPHA")
