from __future__ import annotations

import math

import pytest

from hfa.authority import OperationType
from hfa.dag.schema import DagTaskSeed
from hfa_control.task_admit_authority import (
    FEATURE_FLAG,
    WRITER_ID,
    build_task_admit_command,
    build_task_admit_context,
    parse_task_admit_binding_flag,
    stable_committed_at_ms,
)


def seed(**changes):
    values = dict(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        agent_type="default",
        priority=5,
        admitted_at=1000.0,
        dependency_count=0,
        child_task_ids=("child-b", "child-a"),
        payload_json='{"x":1}',
        trace_parent="trace",
        trace_state="state",
        region="eu",
        policy="standard",
    )
    values.update(changes)
    return DagTaskSeed(**values)


def test_feature_flag_defaults_disabled():
    assert parse_task_admit_binding_flag(None) is False
    assert parse_task_admit_binding_flag("") is False


@pytest.mark.parametrize("value", ["1", " true ", "YES", "on"])
def test_feature_flag_accepts_true_values(value):
    assert parse_task_admit_binding_flag(value) is True


@pytest.mark.parametrize("value", ["0", " false ", "NO", "off"])
def test_feature_flag_accepts_false_values(value):
    assert parse_task_admit_binding_flag(value) is False


def test_feature_flag_rejects_unknown_value():
    with pytest.raises(ValueError, match=FEATURE_FLAG):
        parse_task_admit_binding_flag("maybe")


def test_writer_identity_is_fixed_and_non_overridable():
    command = build_task_admit_command(seed())
    context = build_task_admit_context(command)
    assert context.authenticated_writer_id == WRITER_ID
    assert context.allowed_operations == frozenset({OperationType.TASK_ADMIT})
    assert context.target_aggregate_identity_sha256 == command.aggregate_identity.sha256


def test_operation_id_is_deterministic_from_task_identity():
    first = build_task_admit_command(seed())
    second = build_task_admit_command(seed(priority=99, payload_json='{"x":2}'))
    assert first.operation_id == second.operation_id
    assert first.operation_id == f"task-admit:v1:{first.aggregate_identity.sha256}"


def test_same_seed_produces_same_command_hash():
    assert build_task_admit_command(seed()).canonical_command_hash == build_task_admit_command(seed()).canonical_command_hash


def test_changed_admission_payload_produces_different_command_hash():
    assert build_task_admit_command(seed()).canonical_command_hash != build_task_admit_command(seed(payload_json='{"x":2}')).canonical_command_hash


def test_root_seed_requests_ready_queue_intent():
    command = build_task_admit_command(seed(dependency_count=0))
    assert command.intended_next_state == "ready"
    assert command.requested_projection_intents[0]["kind"] == "READY_QUEUE_IF_READY"


def test_waiting_seed_requests_no_ready_queue_intent():
    command = build_task_admit_command(seed(dependency_count=2))
    assert command.intended_next_state == "pending"
    assert command.requested_projection_intents == ()


@pytest.mark.parametrize("value", [0, 1, 1000.0])
def test_committed_at_ms_accepts_integral_seed_timestamp(value):
    assert stable_committed_at_ms(value) == int(value)


@pytest.mark.parametrize("value", [True, -1, 1.5, math.nan, math.inf, -math.inf, 2**53])
def test_committed_at_ms_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        stable_committed_at_ms(value)

@pytest.mark.parametrize("field,value", [
    ("dependency_count", True), ("dependency_count", 0.5), ("dependency_count", "1"),
    ("priority", True), ("priority", 1.5), ("priority", "5"),
])
def test_dependency_and_priority_require_exact_integers(field, value):
    with pytest.raises(ValueError):
        build_task_admit_command(seed(**{field: value}))


def test_normalized_values_match_canonical_payload():
    command = build_task_admit_command(seed(priority=5.0, dependency_count=2.0))
    assert command.authoritative_payload["priority"] == 5
    assert command.authoritative_payload["dependency_count"] == 2
    assert command.intended_next_state == "pending"

@pytest.mark.asyncio
async def test_legacy_footprint_checks_async_keys_sequentially():
    from hfa_control.task_admit_authority import TaskAdmitAuthorityBinding

    class RedisStub:
        def __init__(self):
            self.exists_calls = []
        async def exists(self, key):
            self.exists_calls.append(key)
            return len(self.exists_calls) == 2
        async def sismember(self, *_args):
            raise AssertionError("must short-circuit after direct footprint")
        async def zscore(self, *_args):
            raise AssertionError("must short-circuit after direct footprint")

    binding = TaskAdmitAuthorityBinding(RedisStub(), legacy_admit=lambda _seed: None, store=object())
    assert await binding._legacy_footprint_exists(seed()) is True
    assert len(binding.redis.exists_calls) == 2


@pytest.mark.asyncio
async def test_flag_off_legacy_projection_preserves_original_float_coercion():
    from hfa_control.dag_lua import DagLua

    class Loader:
        def __init__(self):
            self.args = None
        async def run(self, *, num_keys, keys, args):
            self.args = args
            return ["seeded_root"]

    dag = DagLua(object(), canonical_task_admit_binding=False)
    loader = Loader()
    dag._admit_loader = loader
    dag._initialised = True
    await dag._task_admit_legacy(seed(admitted_at=1000))
    assert loader.args[5] == "1000.0"


@pytest.mark.asyncio
async def test_canonical_projection_uses_exact_normalized_values():
    from hfa_control.dag_lua import DagLua
    from hfa_control.task_admit_authority import normalize_task_admit_seed

    class Loader:
        def __init__(self):
            self.args = None
        async def run(self, *, num_keys, keys, args):
            self.args = args
            return ["seeded_waiting"]

    dag = DagLua(object(), canonical_task_admit_binding=True)
    loader = Loader()
    dag._admit_loader = loader
    dag._initialised = True
    normalized = normalize_task_admit_seed(seed(priority=5.0, admitted_at=1000.0, dependency_count=2.0))
    await dag._task_admit_canonical_projection(normalized)
    assert loader.args[4] == "5"
    assert loader.args[5] == "1000"
    assert loader.args[9] == "2"

@pytest.mark.asyncio
async def test_store_commit_returns_already_applied_then_exact_head_validation_runs(monkeypatch):
    from hfa.authority import RedisAuthorityCommitResult, RedisAuthorityCommitStatus
    from hfa_control import task_admit_authority as module
    from hfa_control.task_admit_authority import TaskAdmitAuthorityBinding

    item = seed(task_id="commit-race")
    command = build_task_admit_command(item)
    evaluation = module.evaluate_authority_commit(
        context=build_task_admit_context(command),
        command=command,
        current_revision=0,
        current_state=None,
        receipt_probe=None,
        committed_at_ms=1000,
        correlation_id=None,
    )
    assert evaluation.commit_plan is not None

    class RedisStub:
        async def exists(self, _key): return False
        async def sismember(self, *_args): return False
        async def zscore(self, *_args): return None

    class StoreStub:
        def __init__(self):
            self.validations = []
        async def initialise(self): pass
        async def get_aggregate_snapshot(self, _identity): return None
        async def load_receipt_probe(self, _identity, _operation_id): return None
        async def commit(self, _plan):
            return RedisAuthorityCommitResult(
                status=RedisAuthorityCommitStatus.ALREADY_APPLIED,
                transition_id=evaluation.commit_plan.record.transition_id,
                aggregate_revision=1,
            )
        def keyspace(self, _identity_sha):
            class Keyspace:
                @staticmethod
                def operation_field(operation_id):
                    import hashlib
                    encoded = operation_id.encode("utf-8")
                    return hashlib.sha256(len(encoded).to_bytes(8, "big") + encoded).hexdigest()
            return Keyspace()
        async def validate_authority_head(self, identity, **expected):
            self.validations.append((identity, expected))
            return object()

    projected = []
    async def legacy(value):
        projected.append(value)
        return "legacy"

    store = StoreStub()
    binding = TaskAdmitAuthorityBinding(RedisStub(), legacy, store=store)
    result = await binding.admit(item)
    assert result == "legacy"
    assert len(store.validations) == 1
    expected = store.validations[0][1]
    record = evaluation.commit_plan.record
    assert expected["expected_operation_id"] == record.operation_id
    assert expected["expected_transition_id"] == record.transition_id
    assert expected["expected_revision"] == record.to_revision
    assert expected["expected_canonical_command_hash"] == record.canonical_command_hash
    assert expected["expected_canonical_record_hash"] == record.canonical_record_hash
    assert len(projected) == 1


@pytest.mark.asyncio
async def test_legacy_footprint_redis_failure_is_structured():
    from hfa_control.task_admit_authority import (
        TaskAdmitAuthorityBinding,
        TaskAdmitAuthorityConflictError,
    )

    class RedisStub:
        async def exists(self, _key):
            raise ConnectionError("redis unavailable")

    binding = TaskAdmitAuthorityBinding(
        RedisStub(), legacy_admit=lambda _seed: None, store=object()
    )
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await binding._legacy_footprint_exists(seed())
    assert raised.value.status == "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"
    assert "legacy footprint inspection failed" in raised.value.detail
