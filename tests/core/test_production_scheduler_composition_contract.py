from __future__ import annotations

import dataclasses
import importlib
from collections.abc import Mapping
from typing import Any, Callable

import pytest

from hfa_control.dag_lua import DagLua
from hfa_control.dag_scheduler_bridge import DagReadyQueue, DagSchedulerDispatchWriter
from hfa_control.models import ControlPlaneConfig
from hfa_control.scheduler import Scheduler
from hfa_control.scheduler_loop import SchedulerLoop
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.service import ControlPlaneService
from hfa_control.worker_reservation import WorkerReservationManager


class _RedisStub:
    pass


class _RegistryStub:
    async def list_all_workers(self, region=None):
        return []


class _ShardsStub:
    async def shard_for_group(self, worker_group: str, run_id: str) -> int:
        return 0


def _resolve_builder() -> Callable[..., Scheduler]:
    candidates = (
        ("hfa_control.scheduler", "build_production_scheduler"),
        ("hfa_control.production_scheduler", "build_production_scheduler"),
        ("hfa_control.service", "build_production_scheduler"),
    )
    for module_name, symbol_name in candidates:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        symbol = getattr(module, symbol_name, None)
        if callable(symbol):
            return symbol

    service_builder = getattr(ControlPlaneService, "_build_scheduler", None)
    assert callable(service_builder), (
        "missing production scheduler builder; expected build_production_scheduler() "
        "or ControlPlaneService._build_scheduler()"
    )
    return service_builder


def _composition_mapping(scheduler: Scheduler) -> Mapping[str, Any]:
    diagnostic = getattr(scheduler, "composition", None)
    if callable(diagnostic):
        diagnostic = diagnostic()
    if diagnostic is None:
        getter = getattr(scheduler, "get_composition", None)
        if callable(getter):
            diagnostic = getter()

    assert diagnostic is not None, (
        "Scheduler must expose a shallow read-only production composition diagnostic"
    )
    if isinstance(diagnostic, Mapping):
        return diagnostic
    if dataclasses.is_dataclass(diagnostic):
        return {
            field.name: getattr(diagnostic, field.name)
            for field in dataclasses.fields(diagnostic)
        }
    public = {
        name: getattr(diagnostic, name)
        for name in dir(diagnostic)
        if not name.startswith("_") and not callable(getattr(diagnostic, name))
    }
    assert public, "composition diagnostic has no readable component fields"
    return public


def _build(builder: Callable[..., Scheduler], redis: Any, config: Any) -> Scheduler:
    kwargs = {
        "redis": redis,
        "config": config,
        "registry": _RegistryStub(),
        "shards": _ShardsStub(),
        "event_store": None,
    }
    try:
        return builder(**kwargs)
    except TypeError as exc:
        pytest.fail(f"production scheduler builder rejected canonical dependencies: {exc}")


def test_control_plane_config_owns_positive_reservation_ttl() -> None:
    config = ControlPlaneConfig(instance_id="cp-sprint-78")
    assert hasattr(config, "scheduler_reservation_ttl_seconds"), (
        "ControlPlaneConfig must own scheduler_reservation_ttl_seconds"
    )
    assert int(config.scheduler_reservation_ttl_seconds) > 0

    with pytest.raises((TypeError, ValueError)):
        ControlPlaneConfig(
            instance_id="cp-sprint-78",
            scheduler_reservation_ttl_seconds=0,
        )


def test_production_scheduler_builder_is_exposed() -> None:
    assert callable(_resolve_builder())


def test_control_plane_service_constructs_real_scheduler_contract() -> None:
    config = ControlPlaneConfig(instance_id="cp-sprint-78")
    try:
        service = ControlPlaneService(_RedisStub(), config)
    except TypeError as exc:
        pytest.fail(f"ControlPlaneService scheduler construction is incompatible: {exc}")

    scheduler = getattr(service, "scheduler", None) or getattr(service, "_scheduler", None)
    assert isinstance(scheduler, Scheduler)


def test_production_graph_contains_canonical_components_only() -> None:
    config = ControlPlaneConfig(instance_id="cp-sprint-78")
    ttl = getattr(config, "scheduler_reservation_ttl_seconds", None)
    assert ttl is not None and int(ttl) > 0

    redis = _RedisStub()
    scheduler = _build(_resolve_builder(), redis, config)
    assert isinstance(scheduler, Scheduler)

    components = _composition_mapping(scheduler)
    required_types = {
        "scheduler_loop": SchedulerLoop,
        "ready_queue": DagReadyQueue,
        "dag_lua": DagLua,
        "dispatch_writer": DagSchedulerDispatchWriter,
        "reservation_manager": WorkerReservationManager,
        "reservation_dispatcher": SchedulerReservationDispatcher,
    }
    for name, expected_type in required_types.items():
        assert isinstance(components.get(name), expected_type), (
            f"production composition missing {name}: {expected_type.__name__}"
        )

    assert components["scheduler_loop"]._explicit_epoch_required is True

    controller = components.get("dispatch_controller")
    assert controller is not None, "canonical DAG dispatch controller is absent"
    assert type(controller).__name__ in {
        "DagSchedulerDispatchController",
        "ProductionSchedulerDispatchController",
    }

    manager = components["reservation_manager"]
    manager_redis = getattr(manager, "redis", None) or getattr(manager, "_redis", None)
    assert manager_redis is redis
    assert getattr(manager, "_scheduler_id", None) == config.instance_id
    assert int(getattr(manager, "_reservation_ttl_seconds", 0)) == int(ttl)

    dispatcher = components["reservation_dispatcher"]
    legacy_redis = getattr(dispatcher, "redis", None)
    if legacy_redis is None:
        legacy_redis = getattr(dispatcher, "_redis", None)
    assert legacy_redis is None, (
        "legacy RedisKey.run_state(run_id) OCC dependency must not be injected"
    )
    assert components.get("tenant_queue") is None
    assert components.get("injected_dispatch_callback") is None

@pytest.mark.asyncio
async def test_dag_ready_queue_active_tenants_are_sorted_and_malformed_members_skipped() -> None:
    class _Redis:
        async def smembers(self, key):
            assert key == "hfa:dag:tenants:active"
            return {b"tenant-b", b"tenant-a", b"\xff", b"", "tenant-a"}

    queue = DagReadyQueue(_Redis())
    assert await queue.list_active_tenants() == ["tenant-a", "tenant-b"]
