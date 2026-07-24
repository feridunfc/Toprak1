from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import pytest
import redis.asyncio as redis_asyncio

LUA_PATH = "hfa-core/src/hfa/lua/task_complete.lua"


def _decode(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _parent_keys(parent_id: str, tenant_id: str) -> dict[str, str]:
    return {
        "state": f"hfa:dag:task:{parent_id}:state",
        "meta": f"hfa:dag:task:{parent_id}:meta",
        "children": f"hfa:dag:task:{parent_id}:children",
        "output": f"hfa:dag:task:{parent_id}:output",
        "ready": f"hfa:dag:tenant:{tenant_id}:ready",
        "running": f"hfa:dag:tenant:{tenant_id}:running",
    }


def _child_keys(child_id: str, tenant_id: str) -> dict[str, str]:
    return {
        "state": f"hfa:dag:task:{child_id}:state",
        "remaining": f"hfa:dag:task:{child_id}:remaining_deps",
        "ready_emitted": f"hfa:dag:task:{child_id}:ready_emitted",
        "ready": f"hfa:dag:tenant:{tenant_id}:ready",
    }


async def _seed_parent(
    redis,
    *,
    parent_id: str,
    child_id: str,
    tenant_id: str,
    run_id: str,
) -> None:
    keys = _parent_keys(parent_id, tenant_id)
    await redis.set(keys["state"], "running", ex=86400)
    await redis.hset(
        keys["meta"],
        mapping={
            "task_id": parent_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": "s80-worker",
            "scheduler_epoch": "s80-scheduler-epoch",
            "claim_epoch": "1",
        },
    )
    await redis.sadd(keys["children"], child_id)
    await redis.zadd(keys["running"], {parent_id: 1})


async def _seed_child(
    redis,
    *,
    child_id: str,
    tenant_id: str,
    state: str,
    remaining: int | None,
    ready_emitted: bool = False,
) -> None:
    keys = _child_keys(child_id, tenant_id)
    await redis.set(keys["state"], state, ex=86400)
    if remaining is not None:
        await redis.set(keys["remaining"], remaining, ex=86400)
    if ready_emitted:
        await redis.set(keys["ready_emitted"], "1", ex=86400)


async def _snapshot(
    redis,
    *,
    parent_id: str,
    child_id: str,
    tenant_id: str,
) -> dict[str, Any]:
    parent = _parent_keys(parent_id, tenant_id)
    child = _child_keys(child_id, tenant_id)
    remaining_raw = await redis.get(child["remaining"])
    return {
        "parent_state": _decode(await redis.get(parent["state"])),
        "child_state": _decode(await redis.get(child["state"])),
        "child_remaining": None if remaining_raw is None else int(_decode(remaining_raw)),
        "child_ready_emitted": bool(await redis.exists(child["ready_emitted"])),
        "ready_queue_member": await redis.zscore(child["ready"], child_id) is not None,
    }


async def _complete_parent(
    redis,
    *,
    repo_root: Path,
    parent_id: str,
    run_id: str,
    tenant_id: str,
    terminal_state: str,
    ready_score: int,
):
    keys = _parent_keys(parent_id, tenant_id)
    source = (repo_root / LUA_PATH).read_text(encoding="utf-8")
    return await redis.eval(
        source,
        6,
        keys["state"],
        keys["meta"],
        keys["children"],
        keys["output"],
        keys["ready"],
        keys["running"],
        parent_id,
        run_id,
        tenant_id,
        terminal_state,
        5000,
        86400,
        86400,
        86400,
        ready_score,
        "SPRINT80_AGGREGATE_BOUNDARY",
        "",
        "",
        "hfa:dag:task:",
        ":state",
        "hfa:dag:task:",
        ":remaining_deps",
        "hfa:dag:task:",
        ":ready_emitted",
        "",
        "",
    )


def _observation(
    model: ModuleType,
    *,
    scenario: str,
    before: dict[str, Any],
    after: dict[str, Any],
    terminal_state: str,
    result: Any,
    boundary_effect: str,
    finding_status: str,
    notes: tuple[str, ...] = (),
):
    return model.make_observation(
        scenario=scenario,
        parent_state_before=before["parent_state"],
        parent_state_after=after["parent_state"],
        parent_terminal_state_requested=terminal_state,
        completion_status=_decode(result[1]),
        completion_committed=int(result[0]) == 1,
        unlocked_count=int(result[2]),
        child_state_before=before["child_state"],
        child_state_after=after["child_state"],
        child_remaining_before=before["child_remaining"],
        child_remaining_after=after["child_remaining"],
        child_ready_emitted_before=before["child_ready_emitted"],
        child_ready_emitted_after=after["child_ready_emitted"],
        ready_queue_member_before=before["ready_queue_member"],
        ready_queue_member_after=after["ready_queue_member"],
        boundary_effect=boundary_effect,
        finding_status=finding_status,
        notes=notes,
    )


async def _run_scenario(
    redis,
    *,
    repo_root: Path,
    model: ModuleType,
    scenario: str,
    parent_id: str,
    child_id: str,
    tenant_id: str,
    run_id: str,
    terminal_state: str,
    ready_score: int,
    boundary_effect: str,
    finding_status: str,
    notes: tuple[str, ...] = (),
):
    before = await _snapshot(
        redis,
        parent_id=parent_id,
        child_id=child_id,
        tenant_id=tenant_id,
    )
    result = await _complete_parent(
        redis,
        repo_root=repo_root,
        parent_id=parent_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state=terminal_state,
        ready_score=ready_score,
    )
    after = await _snapshot(
        redis,
        parent_id=parent_id,
        child_id=child_id,
        tenant_id=tenant_id,
    )
    return _observation(
        model,
        scenario=scenario,
        before=before,
        after=after,
        terminal_state=terminal_state,
        result=result,
        boundary_effect=boundary_effect,
        finding_status=finding_status,
        notes=notes,
    )


async def _build_report(
    *,
    redis_url: str,
    repo_root: Path,
    model: ModuleType,
) -> dict[str, Any]:
    redis = redis_asyncio.Redis.from_url(redis_url, decode_responses=False)
    await redis.ping()
    await redis.flushdb()
    observations = []
    tenant_id = "s80-aggregate-tenant"
    try:
        parent_id = "s80-parent-single"
        child_id = "s80-child-single"
        run_id = "s80-run-single"
        await _seed_parent(
            redis,
            parent_id=parent_id,
            child_id=child_id,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        await _seed_child(
            redis,
            child_id=child_id,
            tenant_id=tenant_id,
            state="pending",
            remaining=1,
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="single_parent_done_unlocks_child",
                parent_id=parent_id,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="done",
                ready_score=1000,
                boundary_effect="CHILD_UNLOCKED",
                finding_status="PASS",
                notes=("parent and child keys mutate in one Lua invocation",),
            )
        )

        parent_one = "s80-parent-one"
        parent_two = "s80-parent-two"
        child_id = "s80-child-two-parents"
        run_id = "s80-run-two-parents"
        for parent_id in (parent_one, parent_two):
            await _seed_parent(
                redis,
                parent_id=parent_id,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
            )
        await _seed_child(
            redis,
            child_id=child_id,
            tenant_id=tenant_id,
            state="pending",
            remaining=2,
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="first_parent_done_decrements_only",
                parent_id=parent_one,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="done",
                ready_score=2000,
                boundary_effect="DEPENDENCY_DECREMENTED",
                finding_status="PASS",
            )
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="duplicate_parent_completion_no_second_decrement",
                parent_id=parent_one,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="done",
                ready_score=2001,
                boundary_effect="NO_CHILD_EFFECT",
                finding_status="EXPECTED_NOOP",
            )
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="second_parent_done_unlocks_child",
                parent_id=parent_two,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="done",
                ready_score=2002,
                boundary_effect="CHILD_UNLOCKED",
                finding_status="PASS",
            )
        )

        parent_id = "s80-parent-failed"
        child_id = "s80-child-failed-parent"
        run_id = "s80-run-failed-parent"
        await _seed_parent(
            redis,
            parent_id=parent_id,
            child_id=child_id,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        await _seed_child(
            redis,
            child_id=child_id,
            tenant_id=tenant_id,
            state="pending",
            remaining=1,
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="failed_parent_does_not_unlock_child",
                parent_id=parent_id,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="failed",
                ready_score=3000,
                boundary_effect="NO_CHILD_EFFECT",
                finding_status="EXPECTED_NOOP",
            )
        )

        parent_id = "s80-parent-nonpending"
        child_id = "s80-child-running"
        run_id = "s80-run-nonpending"
        await _seed_parent(
            redis,
            parent_id=parent_id,
            child_id=child_id,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        await _seed_child(
            redis,
            child_id=child_id,
            tenant_id=tenant_id,
            state="running",
            remaining=1,
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="nonpending_child_is_not_mutated",
                parent_id=parent_id,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="done",
                ready_score=4000,
                boundary_effect="NO_CHILD_EFFECT",
                finding_status="EXPECTED_NOOP",
            )
        )

        parent_id = "s80-parent-missing-counter"
        child_id = "s80-child-missing-counter"
        run_id = "s80-run-missing-counter"
        await _seed_parent(
            redis,
            parent_id=parent_id,
            child_id=child_id,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        await _seed_child(
            redis,
            child_id=child_id,
            tenant_id=tenant_id,
            state="pending",
            remaining=None,
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="missing_remaining_counter_unlocks_fail_open",
                parent_id=parent_id,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="done",
                ready_score=5000,
                boundary_effect="FAIL_OPEN_UNLOCK",
                finding_status="BLOCKING_GAP",
                notes=("DECR on an absent counter creates -1, clamps to zero, then unlocks",),
            )
        )

        parent_id = "s80-parent-ready-marker"
        child_id = "s80-child-ready-marker"
        run_id = "s80-run-ready-marker"
        await _seed_parent(
            redis,
            parent_id=parent_id,
            child_id=child_id,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        await _seed_child(
            redis,
            child_id=child_id,
            tenant_id=tenant_id,
            state="pending",
            remaining=1,
            ready_emitted=True,
        )
        observations.append(
            await _run_scenario(
                redis,
                repo_root=repo_root,
                model=model,
                scenario="ready_marker_strands_pending_child",
                parent_id=parent_id,
                child_id=child_id,
                tenant_id=tenant_id,
                run_id=run_id,
                terminal_state="done",
                ready_score=6000,
                boundary_effect="CHILD_STRANDED",
                finding_status="BLOCKING_GAP",
                notes=("remaining reaches zero but existing ready_emitted blocks state and queue mutation",),
            )
        )

        source = (repo_root / LUA_PATH).read_text(encoding="utf-8")
        report = model.render_report(
            observations,
            metadata={
                "method": "real_redis_parent_completion_entry_point_observation",
                "task_complete_source": LUA_PATH,
                "child_state_guard": "pending_only",
                "child_mutation_inside_parent_completion_lua": True,
                "source_has_xadd": "xadd" in source.lower(),
                "source_has_revision_token": "revision" in source.lower(),
                "run_state_read_by_task_complete": False,
            },
        )
        model.write_json(
            repo_root / "local_out/sprint80/aggregate_boundary.json",
            report,
        )
        return report
    finally:
        await redis.flushdb()
        await redis.aclose()


@pytest.fixture(scope="module")
def aggregate_boundary_report(
    repo_root: Path,
    sprint80_module_loader: Callable[[str], ModuleType],
) -> dict[str, Any]:
    redis_url = os.getenv("SPRINT80_REDIS_URL", "")
    if not redis_url:
        pytest.skip("SPRINT80_REDIS_URL is required for aggregate boundary diagnostics")
    return asyncio.run(
        _build_report(
            redis_url=redis_url,
            repo_root=repo_root,
            model=sprint80_module_loader("aggregate_boundary"),
        )
    )


def _scenario(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(row for row in report["observations"] if row["scenario"] == name)


@pytest.mark.sprint80_reality
def test_parent_completion_directly_mutates_child_boundary(
    aggregate_boundary_report: dict[str, Any],
):
    assert aggregate_boundary_report["observation_count"] == 8
    assert aggregate_boundary_report["cross_task_mutation_observed"] is True
    assert aggregate_boundary_report["global_result"] == (
        "CROSS_TASK_MUTATION_WITHOUT_CHILD_AUTHORITY_RECORD"
    )
    assert aggregate_boundary_report["transaction_boundary"] == "single_redis_lua_call"


@pytest.mark.sprint80_reality
def test_single_parent_done_unlocks_pending_child(
    aggregate_boundary_report: dict[str, Any],
):
    row = _scenario(aggregate_boundary_report, "single_parent_done_unlocks_child")
    assert row["parent_state_before"] == "running"
    assert row["parent_state_after"] == "done"
    assert row["child_state_before"] == "pending"
    assert row["child_state_after"] == "ready"
    assert row["child_remaining_before"] == 1
    assert row["child_remaining_after"] == 0
    assert row["child_ready_emitted_after"] is True
    assert row["ready_queue_member_after"] is True
    assert row["unlocked_count"] == 1


@pytest.mark.sprint80_reality
def test_two_parent_dependency_unlocks_only_after_second_parent(
    aggregate_boundary_report: dict[str, Any],
):
    first = _scenario(aggregate_boundary_report, "first_parent_done_decrements_only")
    second = _scenario(aggregate_boundary_report, "second_parent_done_unlocks_child")
    assert first["child_remaining_before"] == 2
    assert first["child_remaining_after"] == 1
    assert first["child_state_after"] == "pending"
    assert first["unlocked_count"] == 0
    assert second["child_remaining_before"] == 1
    assert second["child_remaining_after"] == 0
    assert second["child_state_after"] == "ready"
    assert second["unlocked_count"] == 1


@pytest.mark.sprint80_reality
def test_duplicate_parent_completion_does_not_decrement_again(
    aggregate_boundary_report: dict[str, Any],
):
    row = _scenario(
        aggregate_boundary_report,
        "duplicate_parent_completion_no_second_decrement",
    )
    assert row["parent_state_before"] == "done"
    assert row["completion_status"] == "already_terminal"
    assert row["completion_committed"] is False
    assert row["child_remaining_before"] == 1
    assert row["child_remaining_after"] == 1
    assert row["unlocked_count"] == 0


@pytest.mark.sprint80_reality
def test_failed_parent_and_nonpending_child_are_noops(
    aggregate_boundary_report: dict[str, Any],
):
    failed = _scenario(aggregate_boundary_report, "failed_parent_does_not_unlock_child")
    nonpending = _scenario(aggregate_boundary_report, "nonpending_child_is_not_mutated")
    assert failed["parent_state_after"] == "failed"
    assert failed["child_state_after"] == "pending"
    assert failed["child_remaining_after"] == 1
    assert failed["unlocked_count"] == 0
    assert nonpending["child_state_before"] == "running"
    assert nonpending["child_state_after"] == "running"
    assert nonpending["child_remaining_after"] == 1
    assert nonpending["unlocked_count"] == 0


@pytest.mark.sprint80_reality
def test_missing_dependency_counter_unlocks_fail_open(
    aggregate_boundary_report: dict[str, Any],
):
    row = _scenario(
        aggregate_boundary_report,
        "missing_remaining_counter_unlocks_fail_open",
    )
    assert row["child_remaining_before"] is None
    assert row["child_remaining_after"] == 0
    assert row["child_state_after"] == "ready"
    assert row["ready_queue_member_after"] is True
    assert row["boundary_effect"] == "FAIL_OPEN_UNLOCK"
    assert row["finding_status"] == "BLOCKING_GAP"


@pytest.mark.sprint80_reality
def test_existing_ready_marker_can_strand_pending_child(
    aggregate_boundary_report: dict[str, Any],
):
    row = _scenario(aggregate_boundary_report, "ready_marker_strands_pending_child")
    assert row["child_remaining_before"] == 1
    assert row["child_remaining_after"] == 0
    assert row["child_ready_emitted_before"] is True
    assert row["child_state_after"] == "pending"
    assert row["ready_queue_member_after"] is False
    assert row["boundary_effect"] == "CHILD_STRANDED"
    assert row["finding_status"] == "BLOCKING_GAP"


@pytest.mark.sprint80_reality
def test_child_mutation_has_no_revision_or_canonical_record(
    aggregate_boundary_report: dict[str, Any],
):
    assert aggregate_boundary_report["child_authority_record_observed"] is False
    assert aggregate_boundary_report["aggregate_revision_observed"] is False
    assert aggregate_boundary_report["metadata"]["source_has_xadd"] is False
    assert aggregate_boundary_report["metadata"]["source_has_revision_token"] is False
    mutated = [
        row
        for row in aggregate_boundary_report["observations"]
        if row["child_state_before"] != row["child_state_after"]
        or row["child_remaining_before"] != row["child_remaining_after"]
    ]
    assert mutated
    assert all(
        row["child_transition_source"] == "task_complete.lua"
        for row in mutated
    )
    assert all(
        row["canonical_transition_record_observed"] is False
        for row in mutated
    )


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Parent task completion currently mutates child task state and dependency keys directly",
)
def test_parent_completion_does_not_mutate_child_aggregate_directly(
    aggregate_boundary_report: dict[str, Any],
):
    assert aggregate_boundary_report["cross_task_mutation_observed"] is False


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Absent remaining_deps currently clamps to zero and unlocks the child",
)
def test_missing_dependency_counter_fails_closed(
    aggregate_boundary_report: dict[str, Any],
):
    row = _scenario(
        aggregate_boundary_report,
        "missing_remaining_counter_unlocks_fail_open",
    )
    assert row["completion_committed"] is False
    assert row["child_state_after"] == "pending"
    assert row["ready_queue_member_after"] is False


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="An existing ready_emitted marker can leave a zero-dependency child pending and unqueued",
)
def test_ready_marker_cannot_strand_zero_dependency_child(
    aggregate_boundary_report: dict[str, Any],
):
    row = _scenario(aggregate_boundary_report, "ready_marker_strands_pending_child")
    assert row["child_state_after"] == "ready"
    assert row["ready_queue_member_after"] is True


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Cross-task child mutations have no observed aggregate revision or canonical transition record",
)
def test_each_child_transition_has_authority_record_and_revision(
    aggregate_boundary_report: dict[str, Any],
):
    mutated = [
        row
        for row in aggregate_boundary_report["observations"]
        if row["child_state_before"] != row["child_state_after"]
        or row["child_remaining_before"] != row["child_remaining_after"]
    ]
    assert mutated
    assert all(
        row["canonical_transition_record_observed"] is True
        for row in mutated
    )
    assert all(row["aggregate_revision_observed"] is True for row in mutated)
