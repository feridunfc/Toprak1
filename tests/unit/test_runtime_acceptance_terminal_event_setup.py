"""Acceptance setup is explicit; retained terminal evidence still fails closed."""
from __future__ import annotations

import importlib

import pytest

from hfa.config.keys import RedisKey
from hfa_control.run_terminal_event_evidence import (
    CONTRACT_PRODUCER_FIELD,
    CONTRACT_SCHEMA_FIELD,
    ensure_terminal_event_index,
)
from tests.integration.test_terminal_task_run_finalization import _dag, _seed_run
from tests.unit.test_f02a1_root_binding import root_redis, snapshot


pytestmark = pytest.mark.asyncio
MODULES = (
    "scripts.runtime_alpha_acceptance_83_5",
    "scripts.runtime_alpha_acceptance_83_6",
)
FAULTS = (
    "missing", "wrong_type", "expiring", "incompatible", "corrupt_evidence",
)
RUN_ID = "run-acceptance-setup-contract"


class ClientProxy:
    def __init__(self, redis):
        self.redis = redis
        self.closed = False

    def __getattr__(self, name):
        return getattr(self.redis, name)

    async def aclose(self):
        self.closed = True


async def damage_index(redis, fault):
    index = RedisKey.run_terminal_event_index()
    await ensure_terminal_event_index(redis)
    if fault == "missing":
        await redis.delete(index)
    elif fault == "wrong_type":
        await redis.delete(index)
        await redis.set(index, "wrong-type")
    elif fault == "expiring":
        await redis.pexpire(index, 600_000)
    elif fault == "incompatible":
        await redis.hset(index, CONTRACT_PRODUCER_FIELD, "999")
    elif fault == "corrupt_evidence":
        await redis.hset(
            index, RedisKey.run_terminal_event_evidence_field(RUN_ID), "not-json",
        )
    else:
        raise AssertionError(fault)


@pytest.mark.parametrize("module_name", MODULES)
async def test_explicit_reset_prepares_only_persistent_contract_before_scenario(
    root_redis, monkeypatch, module_name,
):
    module = importlib.import_module(module_name)
    # A reset discards disposable data before preparing the empty producer shell.
    await root_redis.set("disposable-sentinel", "old-data")
    client = ClientProxy(root_redis)
    monkeypatch.setattr(module.redis_async, "from_url", lambda *a, **kw: client)
    monkeypatch.setenv("HFA_CANONICAL_TASK_ADMIT_BINDING", "retained-value")
    observed = []

    async def scenario(redis, *, acceptance_id):
        assert acceptance_id == "setup-contract"
        assert await redis.keys("*") == [RedisKey.run_terminal_event_index()]
        observed.append(await redis.hgetall(RedisKey.run_terminal_event_index()))
        assert observed == [{CONTRACT_SCHEMA_FIELD: "1", CONTRACT_PRODUCER_FIELD: "1"}]
        assert await redis.ttl(RedisKey.run_terminal_event_index()) == -1
        return {"status": "PASS"}

    monkeypatch.setattr(module, "_http_product_scenario", scenario)
    report = await module.build_acceptance_report(
        "redis://disposable-test-only", acceptance_id="setup-contract", reset_test_db=True,
    )
    assert report["status"] == "PASS_WITH_LIMITATIONS"
    assert len(observed) == 1
    assert client.closed
    assert module.os.environ["HFA_CANONICAL_TASK_ADMIT_BINDING"] == "retained-value"


@pytest.mark.parametrize("module_name", MODULES)
@pytest.mark.parametrize("fault", FAULTS)
async def test_without_reset_does_not_prepare_or_replace_retained_evidence(
    root_redis, monkeypatch, module_name, fault,
):
    module = importlib.import_module(module_name)
    await damage_index(root_redis, fault)
    before = await snapshot(root_redis)
    client = ClientProxy(root_redis)
    monkeypatch.setattr(module.redis_async, "from_url", lambda *a, **kw: client)
    monkeypatch.delenv("HFA_CANONICAL_TASK_ADMIT_BINDING", raising=False)

    async def forbidden_setup(*args, **kwargs):
        raise AssertionError("retained-domain terminal index provisioning is forbidden")

    class ScenarioReached(Exception):
        pass

    async def scenario(redis, **kwargs):
        assert await snapshot(redis) == before
        raise ScenarioReached

    monkeypatch.setattr(module, "ensure_terminal_event_index", forbidden_setup)
    monkeypatch.setattr(module, "_http_product_scenario", scenario)
    with pytest.raises(ScenarioReached):
        await module.build_acceptance_report("redis://disposable-test-only")
    assert await snapshot(root_redis) == before
    assert client.closed
    assert "HFA_CANONICAL_TASK_ADMIT_BINDING" not in module.os.environ


@pytest.mark.parametrize("fault", FAULTS)
async def test_actual_terminal_writer_preserves_evidence_and_denies_ack(root_redis, fault):
    task_id = "task-acceptance-setup-contract"
    await _seed_run(
        root_redis, run_id=RUN_ID, tenant_id="tenant-contract", tasks={task_id: "done"},
    )
    coordinator = await _dag(root_redis)
    await damage_index(root_redis, fault)
    before = await snapshot(root_redis)
    result = await coordinator.finalize_run_from_tasks(
        run_id=RUN_ID, tenant_id="tenant-contract", trigger_task_id=task_id,
        finalized_at_ms=1000, worker_instance_id="worker-contract",
        trigger_terminal_state="done",
    )
    assert result.status == "run_truth_corruption_conflict"
    assert result.finalized is False
    assert result.ack_allowed is False
    # Conflict journaling is permitted; all lifecycle and terminal evidence is immutable.
    journal_keys = {
        RedisKey.runtime_truth_conflict_index().encode(),
        RedisKey.runtime_truth_conflict_stream().encode(),
    }
    after = await snapshot(root_redis)
    assert {k: v for k, v in after.items() if k not in journal_keys} == before
    assert await root_redis.xlen(RedisKey.runtime_truth_conflict_stream()) == 1
    assert await root_redis.xlen(RedisKey.stream_results()) == 0
