from __future__ import annotations

from pathlib import Path

import pytest

from hfa.governance import admission_resource_reservation as reservation_module
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationInput,
    AdmissionResourceReservationManager,
    AdmissionResourceSettlementInput,
    RELEASED_RECEIPT_TTL_SECONDS,
    RESERVATION_STATUS_ALREADY_FINALIZED,
    RESERVATION_STATUS_ALREADY_RELEASED,
    RESERVATION_STATUS_ALREADY_RESERVED,
    RESERVATION_STATUS_ALREADY_SETTLED,
    RESERVATION_STATUS_CONFLICT,
    RESERVATION_STATUS_FINALIZED,
    RESERVATION_STATUS_RELEASED,
    RESERVATION_STATUS_RESERVED,
    RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
    RESERVATION_STATUS_SETTLED,
    RESERVATION_STATUS_STATE_CONFLICT,
)

MAX_SAFE_INTEGER = 2**53 - 1
HEX_A = "a" * 64
HEX_B = "b" * 64
OPERATION_ID = f"run-create:v1:{HEX_A}"


def make_input(**overrides) -> AdmissionResourceReservationInput:
    values = {
        "operation_id": OPERATION_ID,
        "run_id": "run-tenant-a-0001",
        "tenant_id": "tenant-a",
        "estimated_cost_cents": 125,
    }
    values.update(overrides)
    return AdmissionResourceReservationInput(**values)


class ModelLoader:
    """Strict in-memory model for unit-level manager contract tests."""

    def __init__(self) -> None:
        self.loaded = False
        self.receipts: dict[str, dict[str, str]] = {}
        self.strings: dict[str, str] = {}
        self.types: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.mutation_count = 0
        self.calls: list[dict] = []

    async def load(self) -> None:
        self.loaded = True

    def set_counter(self, key: str, value: int | str, *, ttl: int = -1) -> None:
        self.strings[key] = str(value)
        self.types[key] = "string"
        self.ttls[key] = ttl

    def set_wrong_type(self, key: str, redis_type: str = "hash") -> None:
        self.strings.pop(key, None)
        self.types[key] = redis_type
        self.ttls[key] = -1

    @staticmethod
    def _integer(raw: str | None) -> int | None:
        if raw is None or not raw.isdecimal():
            return None
        value = int(raw)
        if value > MAX_SAFE_INTEGER:
            return None
        return value

    def _initial(self, key: str) -> int | None:
        key_type = self.types.get(key)
        if key_type is None:
            return 0
        if key_type != "string" or self.ttls.get(key) != -1:
            return None
        return self._integer(self.strings.get(key))

    def _active(self, keys: list[str], cost: int) -> list[int] | None:
        values: list[int] = []
        for key in keys:
            if self.types.get(key) != "string" or self.ttls.get(key) != -1:
                return None
            value = self._integer(self.strings.get(key))
            if value is None:
                return None
            values.append(value)
        concurrent, budget, inflight = values
        if concurrent < 1 or budget < cost or inflight < 1:
            return None
        return values

    async def run(self, **kwargs):
        self.calls.append(kwargs)
        receipt_key, concurrent_key, budget_key, inflight_key = kwargs["keys"]
        args = kwargs["args"]
        action = args[0]
        operation_id, run_id, tenant_id = args[1:4]
        cost = int(args[4])
        version = args[5]
        proof = args[6]
        now_ms = args[7]
        limits = [int(value) for value in args[8:11]]
        released_ttl = int(args[11])
        resource_keys = [concurrent_key, budget_key, inflight_key]

        receipt = self.receipts.get(receipt_key)
        if receipt is not None:
            immutable = [
                receipt["operation_id"],
                receipt["run_id"],
                receipt["tenant_id"],
                receipt["estimated_cost_cents"],
                receipt["reservation_version"],
                receipt["proof_sha256"],
            ]
            if immutable != [operation_id, run_id, tenant_id, str(cost), version, proof]:
                return [RESERVATION_STATUS_CONFLICT, receipt["state"], "0"]

            state = receipt["state"]
            if action == "settle":
                terminal = args[12:20]
                if state == "SETTLED":
                    stored_terminal = [
                        receipt["run_terminate_operation_id"],
                        receipt["terminal_proof_sha256"],
                        receipt["canonical_transition_id"],
                        receipt["canonical_record_hash"],
                        receipt["canonical_command_hash"],
                        receipt["canonical_revision"],
                        receipt["final_state"],
                        receipt["settlement_proof_sha256"],
                    ]
                    if stored_terminal != terminal:
                        return [RESERVATION_STATUS_CONFLICT, state, "0"]
                    return [RESERVATION_STATUS_ALREADY_SETTLED, state, "0"]
                if state != "FINALIZED":
                    return [RESERVATION_STATUS_STATE_CONFLICT, state, "0"]
                if self.ttls.get(receipt_key) != -1:
                    return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, state, "0"]
                active = self._active(resource_keys, cost)
                if active is None:
                    return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, state, "0"]
                for key, value in zip(
                    resource_keys,
                    [active[0] - 1, active[1] - cost, active[2] - 1],
                ):
                    self.set_counter(key, value)
                receipt.update({
                    "state": "SETTLED",
                    "settled_at_ms": now_ms,
                    "run_terminate_operation_id": terminal[0],
                    "terminal_proof_sha256": terminal[1],
                    "canonical_transition_id": terminal[2],
                    "canonical_record_hash": terminal[3],
                    "canonical_command_hash": terminal[4],
                    "canonical_revision": terminal[5],
                    "final_state": terminal[6],
                    "settlement_proof_sha256": terminal[7],
                })
                self.ttls[receipt_key] = -1
                self.mutation_count += 4
                return [RESERVATION_STATUS_SETTLED, "SETTLED", "1"]

            if action == "reserve":
                if state == "RELEASED":
                    return [RESERVATION_STATUS_ALREADY_RELEASED, state, "0"]
                if state == "SETTLED":
                    return [RESERVATION_STATUS_STATE_CONFLICT, state, "0"]
                if self.ttls.get(receipt_key) != -1 or self._active(resource_keys, cost) is None:
                    return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, state, "0"]
                return [
                    RESERVATION_STATUS_ALREADY_FINALIZED
                    if state == "FINALIZED"
                    else RESERVATION_STATUS_ALREADY_RESERVED,
                    state,
                    "0",
                ]

            if action == "finalize":
                if state == "RELEASED":
                    return [RESERVATION_STATUS_STATE_CONFLICT, state, "0"]
                if state == "SETTLED":
                    return [RESERVATION_STATUS_STATE_CONFLICT, state, "0"]
                if self.ttls.get(receipt_key) != -1 or self._active(resource_keys, cost) is None:
                    return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, state, "0"]
                if state == "FINALIZED":
                    return [RESERVATION_STATUS_ALREADY_FINALIZED, state, "0"]
                receipt["state"] = "FINALIZED"
                receipt["finalized_at_ms"] = now_ms
                self.ttls[receipt_key] = -1
                return [RESERVATION_STATUS_FINALIZED, "FINALIZED", "0"]

            if state == "RELEASED":
                return [RESERVATION_STATUS_ALREADY_RELEASED, state, "0"]
            if state == "SETTLED":
                return [RESERVATION_STATUS_STATE_CONFLICT, state, "0"]
            if state == "FINALIZED":
                return [RESERVATION_STATUS_STATE_CONFLICT, state, "0"]
            if self.ttls.get(receipt_key) != -1:
                return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, state, "0"]
            active = self._active(resource_keys, cost)
            if active is None:
                return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, state, "0"]
            for key, value in zip(
                resource_keys,
                [active[0] - 1, active[1] - cost, active[2] - 1],
            ):
                self.set_counter(key, value)
            receipt["state"] = "RELEASED"
            receipt["released_at_ms"] = now_ms
            self.ttls[receipt_key] = released_ttl
            self.mutation_count += 4
            return [RESERVATION_STATUS_RELEASED, "RELEASED", "1"]

        if action != "reserve":
            return ["reservation_missing", "", "0"]

        current = [self._initial(key) for key in resource_keys]
        if any(value is None for value in current):
            return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, "", "0"]
        concurrent, budget, inflight = current
        if (
            concurrent > MAX_SAFE_INTEGER - 1
            or inflight > MAX_SAFE_INTEGER - 1
            or cost > MAX_SAFE_INTEGER - budget
        ):
            return [RESERVATION_STATUS_RESOURCE_STATE_CONFLICT, "", "0"]

        next_values = [concurrent + 1, budget + cost, inflight + 1]
        for value, limit, status in zip(
            next_values,
            limits,
            [
                "concurrent_run_quota_exceeded",
                "budget_exceeded",
                "tenant_inflight_exceeded",
            ],
        ):
            if limit >= 0 and value > limit:
                return [status, "", "0"]

        for key, value in zip(resource_keys, next_values):
            self.set_counter(key, value)
        self.receipts[receipt_key] = {
            "operation_id": operation_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "estimated_cost_cents": str(cost),
            "reservation_version": version,
            "proof_sha256": proof,
            "state": "RESERVED",
            "created_at_ms": now_ms,
            "finalized_at_ms": "",
            "released_at_ms": "",
        }
        self.ttls[receipt_key] = -1
        self.mutation_count += 4
        return [RESERVATION_STATUS_RESERVED, "RESERVED", "1"]


class DummyRedis:
    pass


def manager_and_model():
    model = ModelLoader()
    manager = AdmissionResourceReservationManager(DummyRedis(), loader=model)
    return manager, model


def limits():
    return {
        "concurrent_run_limit": 10,
        "budget_limit_cents": 10_000,
        "tenant_inflight_limit": 10,
    }


def unbounded_limits():
    return {
        "concurrent_run_limit": None,
        "budget_limit_cents": None,
        "tenant_inflight_limit": None,
    }


def settlement_input(
    reservation: AdmissionResourceReservationInput,
) -> AdmissionResourceSettlementInput:
    return AdmissionResourceSettlementInput(
        run_create_operation_id=reservation.operation_id,
        run_id=reservation.run_id,
        tenant_id=reservation.tenant_id,
        estimated_cost_cents=reservation.estimated_cost_cents,
        run_create_reservation_proof_sha256=reservation.proof_sha256,
        run_terminate_operation_id=f"run-terminate:v1:{HEX_B}",
        terminal_proof_sha256=HEX_A,
        canonical_transition_id="transition-84-8",
        canonical_record_hash=HEX_A,
        canonical_command_hash=HEX_B,
        canonical_revision=2,
        final_state="done",
    )


def test_exact_operation_id_is_accepted():
    assert make_input().operation_id == OPERATION_ID


@pytest.mark.parametrize(
    "operation_id",
    [
        "run-create:v1:",
        "run-create:v1:x",
        f"run-create:v1:{'A' * 64}",
        f"run-create:v1:{'a' * 63}",
        f"run-create:v1:{'a' * 65}",
        f" run-create:v1:{HEX_A}",
        f"run-create:v1:{HEX_A} ",
        f"run-create:v1:{'g' * 64}",
    ],
)
def test_invalid_operation_ids_are_rejected(operation_id):
    with pytest.raises(ValueError, match="operation_id"):
        make_input(operation_id=operation_id)


def test_proof_is_stable_and_changes_with_immutable_input():
    first = make_input()
    assert first.proof_sha256 == make_input().proof_sha256
    assert first.proof_sha256 != make_input(estimated_cost_cents=126).proof_sha256
    assert first.proof_sha256 != make_input(tenant_id="tenant-b").proof_sha256
    assert len(first.proof_sha256) == 64
    assert first.proof_sha256 == first.proof_sha256.lower()


@pytest.mark.parametrize("value", [-1, 2**53, 1.5, "125", True])
def test_invalid_budget_values_are_rejected(value):
    with pytest.raises(ValueError):
        make_input(estimated_cost_cents=value)


@pytest.mark.asyncio
async def test_first_reserve_is_atomic_and_active_keys_are_persistent():
    manager, model = manager_and_model()
    result = await manager.reserve_once(make_input(), now_ms=1000, **limits())
    assert result.status == RESERVATION_STATUS_RESERVED
    assert result.resource_mutated is True
    call = model.calls[-1]
    assert call["num_keys"] == 4
    assert len(call["args"]) == 12
    assert all(model.ttls[key] == -1 for key in call["keys"])
    assert model.mutation_count == 4


@pytest.mark.asyncio
async def test_exact_reserved_retry_validates_resources_and_mutates_nothing():
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    before = model.mutation_count
    result = await manager.reserve_once(reservation, now_ms=2000, **limits())
    assert result.status == RESERVATION_STATUS_ALREADY_RESERVED
    assert result.resource_mutated is False
    assert model.mutation_count == before


@pytest.mark.asyncio
async def test_changed_proof_conflicts_before_resource_mutation():
    manager, model = manager_and_model()
    await manager.reserve_once(make_input(), now_ms=1000, **limits())
    before = model.mutation_count
    result = await manager.reserve_once(
        make_input(run_id="run-tenant-a-changed"), now_ms=2000, **limits()
    )
    assert result.status == RESERVATION_STATUS_CONFLICT
    assert model.mutation_count == before


@pytest.mark.asyncio
async def test_finalize_is_exact_once_and_keeps_active_receipt_persistent():
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    first = await manager.finalize_once(reservation, now_ms=2000)
    second = await manager.finalize_once(reservation, now_ms=3000)
    receipt_key = manager.reservation_receipt_key(reservation.operation_id)
    assert first.status == RESERVATION_STATUS_FINALIZED
    assert second.status == RESERVATION_STATUS_ALREADY_FINALIZED
    assert first.resource_mutated is False
    assert model.ttls[receipt_key] == -1


@pytest.mark.asyncio
async def test_release_is_exact_once_and_only_released_receipt_expires():
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    first = await manager.release_once(reservation, now_ms=2000)
    second = await manager.release_once(reservation, now_ms=3000)
    keys = model.calls[0]["keys"]
    assert first.status == RESERVATION_STATUS_RELEASED
    assert second.status == RESERVATION_STATUS_ALREADY_RELEASED
    assert model.ttls[keys[0]] == RELEASED_RECEIPT_TTL_SECONDS
    assert all(model.ttls[key] == -1 for key in keys[1:])


@pytest.mark.asyncio
async def test_finalize_after_release_and_release_after_finalize_fail_closed():
    manager, _ = manager_and_model()
    released = make_input()
    await manager.reserve_once(released, now_ms=1000, **limits())
    await manager.release_once(released, now_ms=2000)
    assert (await manager.finalize_once(released, now_ms=3000)).status == RESERVATION_STATUS_STATE_CONFLICT

    manager2, _ = manager_and_model()
    finalized = make_input(operation_id=f"run-create:v1:{HEX_B}")
    await manager2.reserve_once(finalized, now_ms=1000, **limits())
    await manager2.finalize_once(finalized, now_ms=2000)
    assert (await manager2.release_once(finalized, now_ms=3000)).status == RESERVATION_STATUS_STATE_CONFLICT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "counter_index,replacement",
    [(0, None), (0, "0"), (1, "124"), (2, None), (2, "0"), (1, "bad")],
)
async def test_release_resource_drift_fails_closed_without_mutation(counter_index, replacement):
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    keys = model.calls[-1]["keys"]
    key = keys[counter_index + 1]
    if replacement is None:
        model.strings.pop(key, None)
        model.types.pop(key, None)
        model.ttls.pop(key, None)
    else:
        model.set_counter(key, replacement)
    before = model.mutation_count
    result = await manager.release_once(reservation, now_ms=2000)
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert model.mutation_count == before
    assert model.receipts[keys[0]]["state"] == "RESERVED"


@pytest.mark.asyncio
async def test_reserved_retry_with_missing_accounting_fails_closed():
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    key = model.calls[-1]["keys"][1]
    model.strings.pop(key)
    model.types.pop(key)
    model.ttls.pop(key)
    result = await manager.reserve_once(reservation, now_ms=2000, **limits())
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False


@pytest.mark.asyncio
@pytest.mark.parametrize("counter_index", [0, 1, 2])
async def test_first_reserve_rejects_existing_expiring_counter_without_normalization(counter_index):
    manager, model = manager_and_model()
    reservation = make_input()
    keys = [
        manager.concurrent_run_key(reservation.tenant_id),
        manager.budget_reserved_key(reservation.tenant_id),
        f"hfa:tenant:{reservation.tenant_id}:inflight",
    ]
    target = keys[counter_index]
    model.set_counter(target, 7, ttl=60)
    before = (dict(model.strings), dict(model.types), dict(model.ttls))
    result = await manager.reserve_once(reservation, now_ms=1000, **limits())
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert (model.strings, model.types, model.ttls) == before
    assert model.receipts == {}
    assert model.mutation_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("counter_index", [0, 1, 2])
async def test_first_reserve_rejects_wrong_redis_type_without_partial_mutation(counter_index):
    manager, model = manager_and_model()
    reservation = make_input()
    keys = [
        manager.concurrent_run_key(reservation.tenant_id),
        manager.budget_reserved_key(reservation.tenant_id),
        f"hfa:tenant:{reservation.tenant_id}:inflight",
    ]
    model.set_wrong_type(keys[counter_index])
    before = (dict(model.strings), dict(model.types), dict(model.ttls))
    result = await manager.reserve_once(reservation, now_ms=1000, **limits())
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert (model.strings, model.types, model.ttls) == before
    assert model.receipts == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "counter_index,counter_value,cost",
    [
        (0, MAX_SAFE_INTEGER, 0),
        (2, MAX_SAFE_INTEGER, 0),
        (1, MAX_SAFE_INTEGER, 1),
        (1, MAX_SAFE_INTEGER - 1, 2),
    ],
)
async def test_safe_integer_addition_overflow_fails_closed(counter_index, counter_value, cost):
    manager, model = manager_and_model()
    reservation = make_input(estimated_cost_cents=cost)
    keys = [
        manager.concurrent_run_key(reservation.tenant_id),
        manager.budget_reserved_key(reservation.tenant_id),
        f"hfa:tenant:{reservation.tenant_id}:inflight",
    ]
    model.set_counter(keys[counter_index], counter_value)
    before = (dict(model.strings), dict(model.types), dict(model.ttls))
    result = await manager.reserve_once(
        reservation, now_ms=1000, **unbounded_limits()
    )
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert (model.strings, model.types, model.ttls) == before
    assert model.receipts == {}


@pytest.mark.asyncio
async def test_safe_integer_exact_boundaries_remain_valid():
    manager, model = manager_and_model()
    reservation = make_input(estimated_cost_cents=1)
    keys = [
        manager.concurrent_run_key(reservation.tenant_id),
        manager.budget_reserved_key(reservation.tenant_id),
        f"hfa:tenant:{reservation.tenant_id}:inflight",
    ]
    model.set_counter(keys[0], MAX_SAFE_INTEGER - 1)
    model.set_counter(keys[1], MAX_SAFE_INTEGER - 1)
    model.set_counter(keys[2], MAX_SAFE_INTEGER - 1)
    result = await manager.reserve_once(
        reservation, now_ms=1000, **unbounded_limits()
    )
    assert result.status == RESERVATION_STATUS_RESERVED
    assert [int(model.strings[key]) for key in keys] == [MAX_SAFE_INTEGER] * 3


@pytest.mark.asyncio
async def test_settle_finalized_is_exact_once_and_keeps_receipt_persistent():
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    await manager.finalize_once(reservation, now_ms=2000)
    settlement = settlement_input(reservation)
    first = await manager.settle_once(settlement, now_ms=3000)
    second = await manager.settle_once(settlement, now_ms=4000)
    receipt_key = manager.reservation_receipt_key(reservation.operation_id)
    assert first.status == RESERVATION_STATUS_SETTLED
    assert first.resource_mutated is True
    assert second.status == RESERVATION_STATUS_ALREADY_SETTLED
    assert second.resource_mutated is False
    assert model.receipts[receipt_key]["state"] == "SETTLED"
    assert model.ttls[receipt_key] == -1


@pytest.mark.asyncio
@pytest.mark.parametrize("counter_index", [0, 1, 2])
async def test_settle_resource_drift_fails_closed_before_any_mutation(counter_index):
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    await manager.finalize_once(reservation, now_ms=2000)
    keys = model.calls[-1]["keys"]
    target = keys[counter_index + 1]
    model.strings.pop(target, None)
    model.types.pop(target, None)
    model.ttls.pop(target, None)
    before = model.mutation_count
    result = await manager.settle_once(settlement_input(reservation), now_ms=3000)
    assert result.status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
    assert result.resource_mutated is False
    assert model.mutation_count == before
    assert model.receipts[keys[0]]["state"] == "FINALIZED"


@pytest.mark.asyncio
async def test_settled_receipt_rejects_changed_terminal_proof_without_mutation():
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    await manager.finalize_once(reservation, now_ms=2000)
    exact = settlement_input(reservation)
    await manager.settle_once(exact, now_ms=3000)
    before = model.mutation_count
    changed = AdmissionResourceSettlementInput(
        **{**exact.__dict__, "canonical_record_hash": HEX_B}
    )
    result = await manager.settle_once(changed, now_ms=4000)
    assert result.status == RESERVATION_STATUS_CONFLICT
    assert result.resource_mutated is False
    assert model.mutation_count == before


@pytest.mark.asyncio
async def test_released_receipt_can_never_be_terminally_settled():
    manager, model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    await manager.release_once(reservation, now_ms=2000)
    result = await manager.settle_once(settlement_input(reservation), now_ms=3000)
    assert result.status == RESERVATION_STATUS_STATE_CONFLICT
    assert model.receipts[manager.reservation_receipt_key(reservation.operation_id)]["state"] == "RELEASED"


@pytest.mark.asyncio
async def test_settled_receipt_cannot_return_to_finalize_or_release():
    manager, _model = manager_and_model()
    reservation = make_input()
    await manager.reserve_once(reservation, now_ms=1000, **limits())
    await manager.finalize_once(reservation, now_ms=2000)
    await manager.settle_once(settlement_input(reservation), now_ms=3000)
    assert (await manager.finalize_once(reservation, now_ms=4000)).status == RESERVATION_STATUS_STATE_CONFLICT
    assert (await manager.release_once(reservation, now_ms=4000)).status == RESERVATION_STATUS_STATE_CONFLICT


def test_lua_contract_contains_terminal_settlement_without_clamp_or_repair():
    lua_path = Path(reservation_module.__file__).resolve().parent.parent / "lua" / "admission_resource_reservation.lua"
    source = lua_path.read_text(encoding="utf-8")
    assert "action == 'settle'" in source
    assert "'state', 'SETTLED'" in source
    assert "'settlement_proof_sha256'" in source
    assert "return {'already_settled', state, '0'}" in source
    assert "redis.call('PERSIST', receipt_key)" in source
    assert "math.max" not in source


def test_lua_contract_closes_ttl_type_and_safe_integer_gaps():
    lua_path = (
        Path(reservation_module.__file__).resolve().parent.parent
        / "lua"
        / "admission_resource_reservation.lua"
    )
    source = lua_path.read_text(encoding="utf-8")
    assert "math.max" not in source
    assert "redis_type_name" in source
    assert "redis.call('TTL', key) ~= -1" in source
    assert "concurrent > MAX_SAFE_INTEGER - 1" in source
    assert "cost > MAX_SAFE_INTEGER - budget" in source
    assert "redis.call('EXPIRE', concurrent_key" not in source
    assert "redis.call('EXPIRE', budget_key" not in source
    assert "redis.call('EXPIRE', inflight_key" not in source
    assert "redis.call('EXPIRE', receipt_key, released_receipt_ttl)" in source
    assert "redis.call('INCRBY', concurrent_key, '1')" in source
    assert "redis.call('INCRBY', budget_key, cost_raw)" in source
    assert "redis.call('DECRBY', budget_key, cost_raw)" in source
    assert "SET clears any prior TTL" not in source


def test_no_legacy_quota_mutation_api_is_exported():
    forbidden = {
        "QuotaManager",
        "check_and_increment_runs",
        "decrement_runs",
        "check_rate_limit",
        "check_and_reserve_budget",
        "HFA_LEGACY_QUOTA_MANAGER_ENABLED",
        "HFA_LEGACY_MAX_CONCURRENT_RUNS",
        "HFA_LEGACY_SYSTEM_RPM_LIMIT",
        "HFA_LEGACY_BUDGET_LIMIT_CENTS",
    }
    assert forbidden.isdisjoint(set(reservation_module.__dict__))
    assert forbidden.isdisjoint(set(reservation_module.__all__))
