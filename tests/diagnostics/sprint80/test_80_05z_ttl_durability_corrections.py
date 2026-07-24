from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import redis.asyncio as redis_asyncio


def _load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load diagnostic module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _decode(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _row(report: dict[str, Any], family: str) -> dict[str, Any]:
    return next(item for item in report["observations"] if item["state_family"] == family)


def _patch_pttl(report: dict[str, Any], family: str, **updates: int) -> None:
    row = _row(report, family)
    values = {stage: int(value) for stage, value in row["pttl_by_transition"]}
    values.update({stage: int(value) for stage, value in updates.items()})
    row["pttl_by_transition"] = [[stage, value] for stage, value in values.items()]


async def _run_isolated_requeue_probe(*, redis_url: str, repo_root: Path) -> dict[str, Any]:
    base = _load_module(
        repo_root / "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py",
        "sprint80_ttl_requeue_cardinality",
    )
    ttl = _load_module(
        repo_root / "tests/diagnostics/sprint80/test_80_05_ttl_durability.py",
        "sprint80_ttl_requeue_probe",
    )
    redis = redis_asyncio.Redis.from_url(redis_url, decode_responses=False)
    await redis.ping()
    await redis.flushdb()
    try:
        task_id = "s80-ttl-requeue-isolated"
        run_id = "s80-ttl-requeue-isolated-run"
        tenant_id = "s80-ttl-requeue-isolated-tenant"
        worker_id = "s80-ttl-requeue-isolated-worker"
        scheduler_epoch = "s80-ttl-requeue-isolated-epoch"
        keys = base._task_keys(task_id, tenant_id, run_id)
        keys["reservation"] = f"hfa:dag:worker:{worker_id}:reservation"
        keys["reservation_owner"] = f"hfa:dag:task:{task_id}:reservation_owner"

        await ttl._admit_with_ttl(
            redis,
            base,
            repo_root,
            keys,
            task_id,
            run_id,
            tenant_id,
            ttl.MAIN_TTL_SECONDS,
        )
        await ttl._dispatch_with_ttl(
            redis,
            base,
            repo_root,
            keys,
            task_id,
            run_id,
            tenant_id,
            ttl.MAIN_TTL_SECONDS,
            scheduler_epoch,
        )
        reserve = await ttl._reserve(
            redis,
            repo_root,
            keys,
            task_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            ttl=ttl.COORDINATION_TTL_SECONDS,
        )
        claim = await ttl._claim_with_ttl(
            redis,
            base,
            repo_root,
            keys,
            task_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            ttl=ttl.MAIN_TTL_SECONDS,
        )
        state_before = _decode(await redis.get(keys["state"]))
        state_pttl_before = int(await redis.pttl(keys["state"]))
        meta_pttl_before = int(await redis.pttl(keys["meta"]))
        ready_pttl_before = int(await redis.pttl(keys["ready"]))
        completion_length_before = int(await redis.xlen(keys["completion_stream"]))

        requeue = await ttl._requeue(
            redis, base, repo_root, keys, task_id, tenant_id
        )
        state_after = _decode(await redis.get(keys["state"]))
        state_pttl_after = int(await redis.pttl(keys["state"]))
        meta_pttl_after = int(await redis.pttl(keys["meta"]))
        ready_pttl_after = int(await redis.pttl(keys["ready"]))
        completion_length_after = int(await redis.xlen(keys["completion_stream"]))
        completion_pttl_after = int(await redis.pttl(keys["completion_stream"]))

        result = {
            "reserve_status": _decode(reserve[0]),
            "claim_status": _decode(claim[0]),
            "requeue_status": _decode(requeue[0]),
            "state_before": state_before,
            "state_after": state_after,
            "state_pttl_before": state_pttl_before,
            "state_pttl_after": state_pttl_after,
            "meta_pttl_before": meta_pttl_before,
            "meta_pttl_after": meta_pttl_after,
            "ready_pttl_before": ready_pttl_before,
            "ready_pttl_after": ready_pttl_after,
            "completion_length_before": completion_length_before,
            "completion_length_after": completion_length_after,
            "completion_pttl_after": completion_pttl_after,
        }

        report_path = repo_root / "local_out/sprint80/ttl_durability.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report["lifecycle"]["after_requeue_state"] = state_after
        report["lifecycle"]["isolated_requeue_probe"] = result
        _patch_pttl(
            report,
            "dag_task_state",
            requeue_before=state_pttl_before,
            requeue_after=state_pttl_after,
        )
        _patch_pttl(
            report,
            "dag_task_meta",
            requeue_before=meta_pttl_before,
            requeue_after=meta_pttl_after,
        )
        _patch_pttl(
            report,
            "ready_queue",
            requeue_before=ready_pttl_before,
            requeue_after=ready_pttl_after,
        )
        _patch_pttl(
            report,
            "completion_stream",
            requeue_after=completion_pttl_after,
        )
        completion_row = _row(report, "completion_stream")
        completion_row["runtime_key_observed"] = True
        completion_row["classification_confidence"] = "runtime_observed"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return result
    finally:
        await redis.flushdb()
        await redis.aclose()


@pytest.fixture(scope="module")
def isolated_requeue_ttl_observation(repo_root: Path) -> dict[str, Any]:
    redis_url = os.getenv("SPRINT80_REDIS_URL", "")
    if not redis_url:
        pytest.skip("SPRINT80_REDIS_URL is required for TTL requeue correction")
    return asyncio.run(
        _run_isolated_requeue_probe(redis_url=redis_url, repo_root=repo_root)
    )


@pytest.mark.sprint80_reality
def test_isolated_requeue_reaches_running_then_ready(
    isolated_requeue_ttl_observation: dict[str, Any],
):
    row = isolated_requeue_ttl_observation
    assert row["reserve_status"] == "reservation_created"
    assert row["claim_status"] == "task_claimed"
    assert row["state_before"] == "running"
    assert row["requeue_status"] == "TASK_REQUEUED"
    assert row["state_after"] == "ready"


@pytest.mark.sprint80_reality
def test_isolated_requeue_removes_state_and_ready_queue_expiry(
    isolated_requeue_ttl_observation: dict[str, Any],
):
    row = isolated_requeue_ttl_observation
    assert row["state_pttl_before"] > 0
    assert row["state_pttl_after"] == -1
    assert row["ready_pttl_after"] == -1
    assert row["meta_pttl_after"] > 0


@pytest.mark.sprint80_reality
def test_completion_stream_retention_is_runtime_observed(
    isolated_requeue_ttl_observation: dict[str, Any],
):
    row = isolated_requeue_ttl_observation
    assert row["completion_length_after"] == row["completion_length_before"] + 1
    assert row["completion_pttl_after"] == -1


@pytest.mark.sprint80_reality
@pytest.mark.sprint80_contract
def test_final_ttl_report_contains_corrected_requeue_observation(
    isolated_requeue_ttl_observation: dict[str, Any],
    repo_root: Path,
):
    report = json.loads(
        (repo_root / "local_out/sprint80/ttl_durability.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["lifecycle"]["after_requeue_state"] == "ready"
    state = next(
        row for row in report["observations"] if row["state_family"] == "dag_task_state"
    )
    values = {stage: int(value) for stage, value in state["pttl_by_transition"]}
    assert values["requeue_after"] == -1
    completion = next(
        row for row in report["observations"] if row["state_family"] == "completion_stream"
    )
    completion_values = {
        stage: int(value) for stage, value in completion["pttl_by_transition"]
    }
    assert completion_values["requeue_after"] == -1
    assert completion["runtime_key_observed"] is True
    assert completion["classification_confidence"] == "runtime_observed"
