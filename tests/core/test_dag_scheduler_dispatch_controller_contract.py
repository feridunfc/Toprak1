from __future__ import annotations

import asyncio
import importlib
import inspect
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from hfa_control.dispatch_controller import DispatchPermit
from hfa_control.scheduler_capability_selector import (
    CapabilitySelectionResult,
    SchedulerCapabilitySelector,
    WorkerCandidate,
)
from hfa_control.scheduler_scoring import SchedulerScoring


class _ReadyQueue:
    def __init__(self, dispatch_input: Any) -> None:
        self.dispatch_input = dispatch_input
        self.calls: list[tuple[str, Any]] = []

    async def list_active_tenants(self) -> list[str]:
        self.calls.append(("list_active_tenants", None))
        return ["tenant-a"]

    async def peek(self, tenant_id: str) -> str:
        self.calls.append(("peek", tenant_id))
        return "task-1"

    async def rebuild_dispatch_input(self, task_id: str, **kwargs):
        self.calls.append(("rebuild_dispatch_input", (task_id, dict(kwargs))))
        return self.dispatch_input


class _Fairness:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def pick_next(self, tenants: list[str]) -> str:
        self.calls.append(list(tenants))
        return tenants[0]


class _PermitController:
    def __init__(self) -> None:
        self.current_calls = 0
        self.consume_calls: list[int] = []

    async def current_permit(self) -> DispatchPermit:
        self.current_calls += 1
        return DispatchPermit(True, None, 1, 1, 1.0)

    async def try_consume(self, n: int = 1) -> bool:
        self.consume_calls.append(n)
        return True


class _ReservationDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def reserve_and_dispatch(self, **kwargs):
        self.calls.append(dict(kwargs))
        return SimpleNamespace(ok=True, status="dispatched")


class _Shards:
    def __init__(self, *, shard: int = 7, error: Exception | None = None) -> None:
        self.shard = shard
        self.error = error
        self.calls: list[tuple[str, str]] = []

    async def shard_for_group(self, worker_group: str, run_id: str) -> int:
        self.calls.append((worker_group, run_id))
        if self.error is not None:
            raise self.error
        return self.shard


def _resolve_controller_type():
    candidates = (
        ("hfa_control.dag_scheduler_dispatch_controller", "DagSchedulerDispatchController"),
        ("hfa_control.production_scheduler_dispatch", "DagSchedulerDispatchController"),
        ("hfa_control.production_scheduler_dispatch", "ProductionSchedulerDispatchController"),
        ("hfa_control.scheduler_dispatch_controller", "DagSchedulerDispatchController"),
    )
    for module_name, symbol_name in candidates:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        symbol = getattr(module, symbol_name, None)
        if isinstance(symbol, type):
            return symbol
    pytest.fail("canonical DAG scheduler dispatch controller is missing")


def _construct_controller(*, ready_queue, fairness, permit, reservation, shards):
    controller_type = _resolve_controller_type()
    values = {
        "ready_queue": ready_queue,
        "dag_ready_queue": ready_queue,
        "tenant_fairness": fairness,
        "fairness": fairness,
        "dispatch_controller": permit,
        "permit_controller": permit,
        "pacing_controller": permit,
        "reservation_dispatcher": reservation,
        "shards": shards,
        "shard_manager": shards,
        "shard_ownership": shards,
    }
    kwargs: dict[str, Any] = {}
    for name, parameter in inspect.signature(controller_type).parameters.items():
        if name in values:
            kwargs[name] = values[name]
        elif parameter.default is inspect.Parameter.empty:
            pytest.fail(
                f"unexpected required dependency {name!r}; controller must not own Redis key layout"
            )
    return controller_type(**kwargs)


def _dispatch_input(*, run_id: str = "run-1", payload=None):
    payload = payload if payload is not None else {}
    return SimpleNamespace(
        task_id="task-1",
        run_id=run_id,
        tenant_id="tenant-a",
        required_capabilities=["gpu"],
        payload=payload,
        dispatch_payload=payload,
        vruntime=1.5,
        inflight=2,
        agent_type="coder",
        region="eu-west-1",
        policy="LEAST_LOADED",
    )


def _snapshot():
    worker = SimpleNamespace(
        worker_id="worker-1",
        worker_group="group-1",
        capabilities=("gpu", "python"),
        capacity=8,
        inflight=2,
        current_load=2,
        schedulable=True,
    )
    return SimpleNamespace(workers=(worker,))


async def _dispatch_once(controller, *, scheduler_epoch: str = "23"):
    method = None
    for name in ("dispatch_once", "try_dispatch_once", "run_once"):
        candidate = getattr(controller, name, None)
        if callable(candidate):
            method = candidate
            break
    assert method is not None, "controller must expose dispatch_once-compatible operation"

    signature = inspect.signature(method)
    kwargs: dict[str, Any] = {}
    if "snapshot" in signature.parameters:
        kwargs["snapshot"] = _snapshot()
    if "worker_scorer" in signature.parameters:
        kwargs["worker_scorer"] = None
    if "scheduler_epoch" in signature.parameters:
        kwargs["scheduler_epoch"] = scheduler_epoch
    return await method(**kwargs)


@pytest.mark.asyncio
async def test_controller_delegates_queue_policy_and_canonical_enrichment(monkeypatch) -> None:
    incoming = {
        "worker_group": "non_authoritative-group",
        "shard": 999,
        "custom": "preserved",
    }
    original = deepcopy(incoming)
    ready = _ReadyQueue(_dispatch_input(payload=incoming))
    fairness = _Fairness()
    permit = _PermitController()
    reservation = _ReservationDispatcher()
    shards = _Shards(shard=7)
    controller = _construct_controller(
        ready_queue=ready,
        fairness=fairness,
        permit=permit,
        reservation=reservation,
        shards=shards,
    )

    compatible = WorkerCandidate(
        worker_id="worker-1",
        worker_group="group-1",
        capabilities=["gpu", "python"],
        capacity=8,
        current_load=2,
    )
    selector_calls: list[dict[str, Any]] = []
    scoring_calls: list[list[Any]] = []

    def fake_filter_workers(*, required_capabilities, workers):
        selector_calls.append({
            "required_capabilities": list(required_capabilities),
            "workers": list(workers),
        })
        return CapabilitySelectionResult([compatible], [], {})

    def fake_choose_best(candidates):
        scoring_calls.append(list(candidates))
        return SimpleNamespace(worker_id="worker-1")

    monkeypatch.setattr(
        SchedulerCapabilitySelector,
        "filter_workers",
        staticmethod(fake_filter_workers),
    )
    monkeypatch.setattr(SchedulerScoring, "choose_best", staticmethod(fake_choose_best))

    assert callable(getattr(controller, "current_permit", None))
    assert callable(getattr(controller, "try_consume", None))
    await controller.current_permit()
    await controller.try_consume(1)
    assert permit.current_calls == 1
    assert permit.consume_calls == [1]

    result = await _dispatch_once(controller, scheduler_epoch="23")
    assert bool(result)
    assert result.dispatched is True
    assert result.status == "committed"
    assert ready.calls[0][0] == "list_active_tenants"
    assert ("peek", "tenant-a") in ready.calls
    assert any(call[0] == "rebuild_dispatch_input" for call in ready.calls)
    assert fairness.calls == [["tenant-a"]]
    assert selector_calls and scoring_calls
    assert shards.calls == [("group-1", "run-1")]

    assert len(reservation.calls) == 1
    call = reservation.calls[0]
    assert call["task_id"] == "task-1"
    assert call["worker_id"] == "worker-1"
    assert call["scheduler_epoch"] == "23"
    payload = call["dispatch_payload"]
    assert payload is not incoming
    assert incoming == original
    assert payload["task_id"] == "task-1"
    assert payload["run_id"] == "run-1"
    assert payload["scheduler_epoch"] == "23"
    assert payload["worker_group"] == "group-1"
    assert payload["shard"] == 7
    assert payload["custom"] == "preserved"


@pytest.mark.asyncio
async def test_missing_run_id_is_not_inferred_from_task_id(monkeypatch) -> None:
    ready = _ReadyQueue(_dispatch_input(run_id=""))
    reservation = _ReservationDispatcher()
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=_PermitController(),
        reservation=reservation,
        shards=_Shards(),
    )
    monkeypatch.setattr(
        SchedulerCapabilitySelector,
        "filter_workers",
        staticmethod(lambda **_: CapabilitySelectionResult([], [], {})),
    )
    result = await _dispatch_once(controller)
    assert result.dispatched is False
    assert result.status == "identity_invalid"
    assert result.reason == "explicit_run_id_required"
    assert reservation.calls == []


@pytest.mark.asyncio
async def test_no_compatible_worker_never_reserves(monkeypatch) -> None:
    ready = _ReadyQueue(_dispatch_input())
    reservation = _ReservationDispatcher()
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=_PermitController(),
        reservation=reservation,
        shards=_Shards(),
    )
    monkeypatch.setattr(
        SchedulerCapabilitySelector,
        "filter_workers",
        staticmethod(
            lambda **_: CapabilitySelectionResult(
                compatible=[],
                rejected_worker_ids=["worker-1"],
                missing_by_worker={"worker-1": ["gpu"]},
            )
        ),
    )
    result = await _dispatch_once(controller)
    assert result.dispatched is False
    assert result.status == "no_worker_available"
    assert result.reason == "no_compatible_workers"
    assert reservation.calls == []


@pytest.mark.asyncio
async def test_missing_shard_ownership_fails_before_reservation(monkeypatch) -> None:
    ready = _ReadyQueue(_dispatch_input())
    reservation = _ReservationDispatcher()
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=_PermitController(),
        reservation=reservation,
        shards=_Shards(error=RuntimeError("no shard ownership")),
    )
    compatible = WorkerCandidate(
        worker_id="worker-1",
        worker_group="group-1",
        capabilities=["gpu"],
        capacity=8,
        current_load=2,
    )
    monkeypatch.setattr(
        SchedulerCapabilitySelector,
        "filter_workers",
        staticmethod(lambda **_: CapabilitySelectionResult([compatible], [], {})),
    )
    monkeypatch.setattr(
        SchedulerScoring,
        "choose_best",
        staticmethod(lambda candidates: SimpleNamespace(worker_id="worker-1")),
    )

    assert not bool(await _dispatch_once(controller))
    assert reservation.calls == []

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        ({"task_id": "wrong-task"}, "payload_task_id_mismatch"),
        ({"run_id": "wrong-run"}, "payload_run_id_mismatch"),
        ({"scheduler_epoch": "22"}, "payload_scheduler_epoch_mismatch"),
    ],
)
async def test_conflicting_incoming_identity_fails_closed(
    monkeypatch,
    payload,
    expected_reason,
) -> None:
    ready = _ReadyQueue(_dispatch_input(payload=payload))
    reservation = _ReservationDispatcher()
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=_PermitController(),
        reservation=reservation,
        shards=_Shards(),
    )

    result = await _dispatch_once(controller, scheduler_epoch="23")
    assert result.dispatched is False
    assert result.status == "identity_invalid"
    assert result.reason == expected_reason
    assert reservation.calls == []


@pytest.mark.asyncio
async def test_dispatch_result_preserves_reservation_failure_category(monkeypatch) -> None:
    ready = _ReadyQueue(_dispatch_input())
    reservation = _ReservationDispatcher()

    async def reject(**kwargs):
        reservation.calls.append(dict(kwargs))
        return SimpleNamespace(ok=False, status="worker_reservation_conflict", reason="busy")

    reservation.reserve_and_dispatch = reject
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=_PermitController(),
        reservation=reservation,
        shards=_Shards(),
    )
    compatible = WorkerCandidate(
        worker_id="worker-1",
        worker_group="group-1",
        capabilities=["gpu"],
        capacity=8,
        current_load=2,
    )
    monkeypatch.setattr(
        SchedulerCapabilitySelector,
        "filter_workers",
        staticmethod(lambda **_: CapabilitySelectionResult([compatible], [], {})),
    )
    monkeypatch.setattr(
        SchedulerScoring,
        "choose_best",
        staticmethod(lambda candidates: SimpleNamespace(worker_id="worker-1")),
    )

    result = await _dispatch_once(controller)
    assert result.dispatched is False
    assert result.status == "reservation_failed"
    assert result.reason == "busy"

@pytest.mark.asyncio
async def test_dispatch_result_preserves_writer_rejection_category(monkeypatch) -> None:
    ready = _ReadyQueue(_dispatch_input())
    reservation = _ReservationDispatcher()

    async def reject(**kwargs):
        reservation.calls.append(dict(kwargs))
        return SimpleNamespace(ok=False, status="dispatch_task_id_mismatch", reason="bad identity")

    reservation.reserve_and_dispatch = reject
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=_PermitController(),
        reservation=reservation,
        shards=_Shards(),
    )
    compatible = WorkerCandidate(
        worker_id="worker-1",
        worker_group="group-1",
        capabilities=["gpu"],
        capacity=8,
        current_load=2,
    )
    monkeypatch.setattr(
        SchedulerCapabilitySelector,
        "filter_workers",
        staticmethod(lambda **_: CapabilitySelectionResult([compatible], [], {})),
    )
    monkeypatch.setattr(
        SchedulerScoring,
        "choose_best",
        staticmethod(lambda candidates: SimpleNamespace(worker_id="worker-1")),
    )

    result = await _dispatch_once(controller)
    assert result.dispatched is False
    assert result.status == "writer_rejected"
    assert result.reason == "bad identity"

@pytest.mark.asyncio
async def test_permit_unavailable_is_observable_without_losing_reason() -> None:
    ready = _ReadyQueue(_dispatch_input())
    permit = _PermitController()

    async def denied():
        permit.current_calls += 1
        return DispatchPermit(False, "dispatch_budget_exhausted", 0, 0, 1.0)

    permit.current_permit = denied
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=permit,
        reservation=_ReservationDispatcher(),
        shards=_Shards(),
    )

    observed = await controller.current_permit()
    assert observed.allowed is False
    assert controller.last_result.status == "permit_unavailable"
    assert controller.last_result.reason == "dispatch_budget_exhausted"


@pytest.mark.asyncio
async def test_cancellation_is_recorded_and_propagated() -> None:
    ready = _ReadyQueue(_dispatch_input())

    async def cancel():
        raise asyncio.CancelledError

    ready.list_active_tenants = cancel
    controller = _construct_controller(
        ready_queue=ready,
        fairness=_Fairness(),
        permit=_PermitController(),
        reservation=_ReservationDispatcher(),
        shards=_Shards(),
    )

    with pytest.raises(asyncio.CancelledError):
        await _dispatch_once(controller)
    assert controller.last_result.status == "cancelled"
    assert controller.last_result.reason == "dispatch_cancelled"
