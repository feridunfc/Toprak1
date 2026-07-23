from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Mapping

import pytest
import redis.asyncio as redis_asyncio

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
)
from hfa_control.terminal_duplicate_cleanup_command import (
    CLEANED,
    execute_terminal_duplicate_cleanup_command,
)

EXPECTED_CORRECTED_OPERATIONS = {
    "task_admit",
    "task_dispatch",
    "task_claim",
    "task_heartbeat",
    "task_complete",
    "task_requeue",
    "legacy_run_completion_sequence",
    "worker_terminal_duplicate_ack",
    "operator_terminal_duplicate_cleanup_command",
}

SUPERSEDED_BASE_TESTS = {
    "test_exact_cardinality_operations_are_observed",
    "test_terminal_duplicate_cleanup_only_acks_transport",
    "test_no_operation_exposes_aggregate_revision_evidence",
}


def _load_base_cardinality_module(repo_root: Path) -> ModuleType:
    path = repo_root / "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py"
    name = "sprint80_base_cardinality_probe"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load base cardinality probe: {path}")
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


async def _hash(redis, key: str) -> dict[str, str]:
    return {
        _decode(field): _decode(value)
        for field, value in (await redis.hgetall(key) or {}).items()
    }


def _record_field_names(observation) -> set[str]:
    names: set[str] = set()
    for record in observation.records:
        names.update(record.fields)
    return names


def _revision_kwargs(
    model: ModuleType,
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    observation,
) -> dict[str, Any]:
    return model.derive_revision_evidence(
        before,
        after,
        additional_field_names=_record_field_names(observation),
    )


async def _capture_base_observations(redis, repo_root: Path, model: ModuleType):
    base = _load_base_cardinality_module(repo_root)
    captured: list[Any] = []
    original_render = model.render_report

    def capture(observations):
        captured.extend(list(observations))
        return {"captured": True}

    model.render_report = capture
    try:
        await base._build_report(redis, repo_root, model)
    finally:
        model.render_report = original_render
    return base, captured


async def _derive_operation_revision_observations(
    redis,
    repo_root: Path,
    model: ModuleType,
    base: ModuleType,
    observations: list[Any],
) -> dict[str, dict[str, Any]]:
    by_name = {row.operation: row for row in observations}
    derived: dict[str, dict[str, Any]] = {}
    await redis.flushdb()

    task_id = "s80-revision-main"
    run_id = "s80-revision-main-run"
    tenant_id = "s80-revision-main-tenant"
    keys = base._task_keys(task_id, tenant_id, run_id)

    before = await _hash(redis, keys["meta"])
    await base._admit(redis, repo_root, keys, task_id, run_id, tenant_id)
    after = await _hash(redis, keys["meta"])
    derived["task_admit"] = _revision_kwargs(
        model, before=before, after=after, observation=by_name["task_admit"]
    )

    before = after
    await base._dispatch(redis, repo_root, keys, task_id, run_id, tenant_id)
    after = await _hash(redis, keys["meta"])
    derived["task_dispatch"] = _revision_kwargs(
        model, before=before, after=after, observation=by_name["task_dispatch"]
    )

    before = after
    await base._claim(redis, repo_root, keys, task_id)
    after = await _hash(redis, keys["meta"])
    derived["task_claim"] = _revision_kwargs(
        model, before=before, after=after, observation=by_name["task_claim"]
    )

    before = after
    await base._heartbeat(redis, repo_root, keys, task_id, tenant_id)
    after = await _hash(redis, keys["meta"])
    derived["task_heartbeat"] = _revision_kwargs(
        model, before=before, after=after, observation=by_name["task_heartbeat"]
    )

    before = after
    await base._complete(redis, repo_root, keys, task_id, run_id, tenant_id)
    after = await _hash(redis, keys["meta"])
    derived["task_complete"] = _revision_kwargs(
        model, before=before, after=after, observation=by_name["task_complete"]
    )

    requeue_task = "s80-revision-requeue"
    requeue_run = "s80-revision-requeue-run"
    requeue_tenant = "s80-revision-requeue-tenant"
    requeue_keys = base._task_keys(requeue_task, requeue_tenant, requeue_run)
    await base._admit(
        redis, repo_root, requeue_keys, requeue_task, requeue_run, requeue_tenant
    )
    await base._dispatch(
        redis, repo_root, requeue_keys, requeue_task, requeue_run, requeue_tenant
    )
    await base._claim(redis, repo_root, requeue_keys, requeue_task)
    before = await _hash(redis, requeue_keys["meta"])
    await base._eval_lua(
        redis,
        repo_root=repo_root,
        script_name="task_requeue",
        keys=[
            requeue_keys["state"],
            requeue_keys["meta"],
            requeue_keys["ready"],
            requeue_keys["running"],
            requeue_keys["completion_stream"],
        ],
        args=[
            requeue_task,
            requeue_tenant,
            "running",
            6000,
            6000,
            3,
            "STALE_HEARTBEAT",
            10000,
        ],
    )
    after = await _hash(redis, requeue_keys["meta"])
    derived["task_requeue"] = _revision_kwargs(
        model, before=before, after=after, observation=by_name["task_requeue"]
    )

    await redis.flushdb()
    legacy_observation = await base._observe_legacy_run_complete(redis, model)
    legacy_fields = {}
    legacy_fields.update(await _hash(redis, "hfa:run:meta:s80-card-legacy-run"))
    legacy_fields.update(await _hash(redis, "hfa:run:result:s80-card-legacy-run"))
    derived["legacy_run_complete"] = _revision_kwargs(
        model,
        before={},
        after=legacy_fields,
        observation=legacy_observation,
    )

    await redis.flushdb()
    worker_ack_observation = await base._observe_terminal_duplicate_cleanup(redis, model)
    worker_meta = await _hash(
        redis, DagRedisKey.task_meta("s80-card-terminal-duplicate")
    )
    derived["terminal_duplicate_cleanup"] = _revision_kwargs(
        model,
        before={},
        after=worker_meta,
        observation=worker_ack_observation,
    )
    return derived


async def _read_new_records(
    redis,
    *,
    key: str,
    start_length: int,
    source: str,
    model: ModuleType,
):
    rows = await redis.xrange(key, min="-", max="+")
    records = []
    for _, raw_fields in rows[start_length:]:
        records.append(
            model.classify_record(
                source=source,
                source_key=key,
                fields=model.decode_mapping(raw_fields),
            )
        )
    return tuple(records)


async def _observe_operator_cleanup(redis, model: ModuleType):
    await redis.flushdb()
    task_id = "s80-operator-cleanup-task"
    run_id = "s80-operator-cleanup-run"
    stream = "s80:operator-cleanup:runtime"
    group = "s80-operator-cleanup-group"
    consumer = "s80-operator-cleanup-consumer"
    state_key = DagRedisKey.task_state(task_id)
    meta_key = DagRedisKey.task_meta(task_id)

    await redis.set(state_key, "done")
    await redis.hset(meta_key, mapping={"task_id": task_id, "run_id": run_id})
    await redis.xgroup_create(stream, group, id="0", mkstream=True)
    message_id = await redis.xadd(
        stream,
        {"event_type": "TaskRequested", "task_id": task_id, "run_id": run_id},
    )
    await redis.xreadgroup(group, consumer, {stream: ">"}, count=1)

    state_before = _decode(await redis.get(state_key))
    meta_before = await _hash(redis, meta_key)
    pending_before = await redis.xpending_range(stream, group, "-", "+", 10)
    runtime_length_before = await redis.xlen(stream)
    audit_length_before = await redis.xlen(TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM)

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id=task_id,
        stream_key=stream,
        consumer_group=group,
        pending_message_id=_decode(message_id),
        dry_run=False,
        execute=True,
        reason="Sprint 80 executable cardinality observation",
    )

    pending_after = await redis.xpending_range(stream, group, "-", "+", 10)
    meta_after = await _hash(redis, meta_key)
    runtime_records = await _read_new_records(
        redis,
        key=stream,
        start_length=runtime_length_before,
        source="shard_stream",
        model=model,
    )
    audit_records = await _read_new_records(
        redis,
        key=TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
        start_length=audit_length_before,
        source="terminal_duplicate_cleanup_audit",
        model=model,
    )
    records = runtime_records + audit_records
    counts = Counter(record.primary_class for record in records)
    revision = model.derive_revision_evidence(
        meta_before,
        meta_after,
        additional_field_names={
            field for record in records for field in record.fields
        },
    )

    assert result.status == CLEANED
    assert result.ack_count == 1
    assert len(pending_before) == 1
    assert len(pending_after) == 0

    phases = {
        _decode(fields.get(b"event_phase") or fields.get("event_phase"))
        for _, fields in await redis.xrange(
            TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM, min="-", max="+"
        )
    }
    assert phases == {AUDIT_PHASE_INTENT, AUDIT_PHASE_OUTCOME}

    return model.OperationObservation(
        operation="operator_terminal_duplicate_cleanup_command",
        transaction_boundary=(
            "multi_command_audit_intent_evidence_recheck_xack_audit_outcome"
        ),
        state_before=state_before,
        state_after=_decode(await redis.get(state_key)),
        aggregate_revision_candidate=revision["aggregate_revision_evidence_count"],
        execution_lifecycle_mutation=0,
        coordination_mutation=1,
        projection_mutation=0,
        transport_append=counts["TransportMessage"],
        transport_ack=result.ack_count,
        audit_append=counts["AuditEvent"],
        canonical_transition_record_count=counts["CanonicalTransitionRecord"],
        changed_keys=(stream, TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM),
        records=records,
        notes=(
            "operator command writes audit intent before XACK",
            "operator command writes audit outcome after XACK",
            "task state and task metadata remain unchanged",
        ),
        **revision,
    )


async def _build_corrected_report(
    *,
    redis_url: str,
    repo_root: Path,
    model: ModuleType,
) -> dict[str, Any]:
    redis = redis_asyncio.Redis.from_url(redis_url, decode_responses=False)
    await redis.ping()
    try:
        base, observations = await _capture_base_observations(
            redis, repo_root, model
        )
        revisions = await _derive_operation_revision_observations(
            redis, repo_root, model, base, observations
        )

        corrected = []
        for observation in observations:
            revision = revisions[observation.operation]
            operation_name = {
                "legacy_run_complete": "legacy_run_completion_sequence",
                "terminal_duplicate_cleanup": "worker_terminal_duplicate_ack",
            }.get(observation.operation, observation.operation)
            corrected.append(
                replace(
                    observation,
                    operation=operation_name,
                    aggregate_revision_candidate=revision[
                        "aggregate_revision_evidence_count"
                    ],
                    **revision,
                )
            )

        corrected.append(await _observe_operator_cleanup(redis, model))
        report = model.render_report(corrected)
        model.write_json(
            repo_root / "local_out/sprint80/transition_cardinality.json",
            report,
        )
        return report
    finally:
        await redis.flushdb()
        await redis.aclose()


@pytest.fixture(scope="module")
def corrected_cardinality_report(
    repo_root: Path,
    sprint80_module_loader: Callable[[str], ModuleType],
):
    redis_url = os.getenv("SPRINT80_REDIS_URL", "")
    if not redis_url:
        pytest.skip("SPRINT80_REDIS_URL is required for corrected cardinality diagnostics")
    model = sprint80_module_loader("transition_cardinality")
    return asyncio.run(
        _build_corrected_report(
            redis_url=redis_url,
            repo_root=repo_root,
            model=model,
        )
    )


def _operation(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(row for row in report["operations"] if row["operation"] == name)


@pytest.mark.sprint80_reality
def test_corrected_operation_scope_contains_nine_operations(
    corrected_cardinality_report: dict[str, Any],
):
    assert corrected_cardinality_report["operation_count"] == 9
    assert {
        row["operation"] for row in corrected_cardinality_report["operations"]
    } == EXPECTED_CORRECTED_OPERATIONS


@pytest.mark.sprint80_reality
def test_worker_terminal_duplicate_ack_emits_no_new_record(
    corrected_cardinality_report: dict[str, Any],
):
    operation = _operation(
        corrected_cardinality_report, "worker_terminal_duplicate_ack"
    )
    assert operation["state_before"] == "done"
    assert operation["state_after"] == "done"
    assert operation["transport_ack"] == 1
    assert operation["audit_append"] == 0
    assert operation["records"] == []


@pytest.mark.sprint80_reality
def test_operator_terminal_duplicate_cleanup_emits_intent_and_outcome_audit(
    corrected_cardinality_report: dict[str, Any],
):
    operation = _operation(
        corrected_cardinality_report,
        "operator_terminal_duplicate_cleanup_command",
    )
    assert operation["state_before"] == "done"
    assert operation["state_after"] == "done"
    assert operation["execution_lifecycle_mutation"] == 0
    assert operation["transport_ack"] == 1
    assert operation["audit_append"] == 2
    assert [record["primary_class"] for record in operation["records"]] == [
        "AuditEvent",
        "AuditEvent",
    ]
    assert corrected_cardinality_report["record_class_counts"]["AuditEvent"] == 2


@pytest.mark.sprint80_reality
def test_revision_evidence_is_derived_from_observed_fields(
    corrected_cardinality_report: dict[str, Any],
):
    assert corrected_cardinality_report["aggregate_revision_evidence_count"] == 0
    assert corrected_cardinality_report["aggregate_revision_evidence"] == []
    for operation in corrected_cardinality_report["operations"]:
        assert "revision_fields_observed" in operation
        assert "revision_semantics" in operation
        assert not operation["aggregate_revision_evidence"]
        assert operation["aggregate_revision_evidence_count"] == 0
        assert operation["aggregate_revision_candidate"] == 0


@pytest.mark.sprint80_reality
def test_claim_epoch_is_not_aggregate_revision(
    corrected_cardinality_report: dict[str, Any],
):
    claim = _operation(corrected_cardinality_report, "task_claim")
    semantics = dict(claim["revision_semantics"])
    assert "claim_epoch" in claim["revision_fields_observed"]
    assert semantics["claim_epoch"] == "coordination_fence"
    assert "claim_epoch" not in claim["aggregate_revision_evidence"]


@pytest.mark.sprint80_reality
def test_scheduler_epoch_is_not_aggregate_revision(
    corrected_cardinality_report: dict[str, Any],
):
    dispatch = _operation(corrected_cardinality_report, "task_dispatch")
    semantics = dict(dispatch["revision_semantics"])
    assert "scheduler_epoch" in dispatch["revision_fields_observed"]
    assert semantics["scheduler_epoch"] == "coordination_fence"
    assert "scheduler_epoch" not in dispatch["aggregate_revision_evidence"]


@pytest.mark.sprint80_reality
def test_field_shape_alone_does_not_verify_canonical_transition_record(
    sprint80_module_loader: Callable[[str], ModuleType],
):
    model = sprint80_module_loader("transition_cardinality")
    fields = {
        "aggregate_id": "agg-1",
        "aggregate_type": "Criterion",
        "from_revision": "1",
        "to_revision": "2",
        "command_id": "cmd-1",
        "transition_type": "CriterionProven",
        "idempotency_key": "idem-1",
        "actor": "kernel-test",
        "occurred_at": "2026-07-23T00:00:00Z",
        "payload_hash": "abc",
    }
    record = model.classify_record(
        source="untrusted_stream",
        source_key="s80:shape-only",
        fields=fields,
        canonical_authority_contract_match=False,
        transaction_coupled=False,
    )
    assert record.canonical_schema_candidate_match is True
    assert record.canonical_authority_contract_match is False
    assert record.transaction_coupled is False
    assert record.verified_canonical_transition_record is False
    assert record.primary_class != "CanonicalTransitionRecord"


@pytest.mark.sprint80_reality
def test_corrected_taxonomy_and_verified_canonical_count(
    corrected_cardinality_report: dict[str, Any],
):
    assert corrected_cardinality_report["record_class_counts"] == {
        "AuditEvent": 2,
        "CanonicalTransitionRecord": 0,
        "EffectResultMessage": 1,
        "EvidenceArtifact": 0,
        "RuntimeTransitionNotification": 2,
        "TransportMessage": 1,
        "Unknown": 0,
    }
    assert corrected_cardinality_report["canonical_transition_record_count"] == 0
    assert corrected_cardinality_report["verified_canonical_transition_records"] == []


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Observed lifecycle mutations still do not produce one transaction-coupled, "
        "authority-verified CanonicalTransitionRecord"
    ),
)
def test_each_lifecycle_mutation_produces_one_verified_canonical_transition_record(
    corrected_cardinality_report: dict[str, Any],
):
    lifecycle_operations = [
        operation
        for operation in corrected_cardinality_report["operations"]
        if operation["execution_lifecycle_mutation"] > 0
    ]
    assert lifecycle_operations
    assert all(
        operation["canonical_transition_record_count"] == 1
        for operation in lifecycle_operations
    )
