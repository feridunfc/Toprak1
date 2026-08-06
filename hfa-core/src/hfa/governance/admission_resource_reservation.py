"""Operation-scoped admission resource reservation primitive.

Sprint 84.4A-R2 provides only the dormant Redis/Lua primitive required by a
future canonical RUN_CREATE binding. It intentionally does not define the
legacy ``QuotaManager`` import surface and is not wired into AdmissionController.
"""
from __future__ import annotations

import hashlib
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from hfa.config.keys import RedisKey
from hfa.lua.loader import LuaScriptLoader

__all__ = [
    "AdmissionResourceReservationConflictError",
    "AdmissionResourceReservationError",
    "AdmissionResourceReservationInput",
    "AdmissionResourceReservationManager",
    "AdmissionResourceReservationReceipt",
    "AdmissionResourceReservationResult",
    "RELEASED_RECEIPT_TTL_SECONDS",
    "RESERVATION_STATE_FINALIZED",
    "RESERVATION_STATE_RELEASED",
    "RESERVATION_STATE_RESERVED",
    "RESERVATION_STATUS_ALREADY_FINALIZED",
    "RESERVATION_STATUS_ALREADY_RELEASED",
    "RESERVATION_STATUS_ALREADY_RESERVED",
    "RESERVATION_STATUS_BUDGET_EXCEEDED",
    "RESERVATION_STATUS_CONFLICT",
    "RESERVATION_STATUS_FINALIZED",
    "RESERVATION_STATUS_INFLIGHT_EXCEEDED",
    "RESERVATION_STATUS_INVALID_INPUT",
    "RESERVATION_STATUS_MISSING",
    "RESERVATION_STATUS_QUOTA_EXCEEDED",
    "RESERVATION_STATUS_RELEASED",
    "RESERVATION_STATUS_RESERVED",
    "RESERVATION_STATUS_RESOURCE_STATE_CONFLICT",
    "RESERVATION_STATUS_STATE_CONFLICT",
]

_MAX_SAFE_INTEGER = 2**53 - 1
_RESERVATION_VERSION = 1
_OPERATION_ID_PATTERN = re.compile(r"^run-create:v1:[0-9a-f]{64}$")
RELEASED_RECEIPT_TTL_SECONDS = 7 * 24 * 60 * 60

RESERVATION_STATUS_RESERVED = "reserved"
RESERVATION_STATUS_ALREADY_RESERVED = "already_reserved"
RESERVATION_STATUS_FINALIZED = "finalized"
RESERVATION_STATUS_ALREADY_FINALIZED = "already_finalized"
RESERVATION_STATUS_RELEASED = "released"
RESERVATION_STATUS_ALREADY_RELEASED = "already_released"
RESERVATION_STATUS_CONFLICT = "reservation_conflict"
RESERVATION_STATUS_STATE_CONFLICT = "reservation_state_conflict"
RESERVATION_STATUS_RESOURCE_STATE_CONFLICT = "resource_state_conflict"
RESERVATION_STATUS_MISSING = "reservation_missing"
RESERVATION_STATUS_QUOTA_EXCEEDED = "concurrent_run_quota_exceeded"
RESERVATION_STATUS_BUDGET_EXCEEDED = "budget_exceeded"
RESERVATION_STATUS_INFLIGHT_EXCEEDED = "tenant_inflight_exceeded"
RESERVATION_STATUS_INVALID_INPUT = "invalid_input"

RESERVATION_STATE_RESERVED = "RESERVED"
RESERVATION_STATE_FINALIZED = "FINALIZED"
RESERVATION_STATE_RELEASED = "RELEASED"


def _required_exact_text(value: Any, field_name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a string")
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    if value != value.strip():
        raise ValueError(f"{field_name} must not contain surrounding whitespace")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{field_name} must already be NFC-normalized")
    return value


def _safe_integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is not int:
        raise ValueError(f"{field_name} must be an exact integer")
    if value < minimum or value > _MAX_SAFE_INTEGER:
        raise ValueError(f"{field_name} is outside the safe integer domain")
    return value


def _optional_limit(value: int | None, field_name: str) -> int:
    if value is None:
        return -1
    return _safe_integer(value, field_name)


def _length_prefixed_utf8(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return len(encoded).to_bytes(8, "big") + encoded


def _component_sha256(*components: str) -> str:
    digest = hashlib.sha256()
    for component in components:
        digest.update(_length_prefixed_utf8(component))
    return digest.hexdigest()


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    if value is None:
        return ""
    return str(value)


def _decode_hash(raw: Mapping[Any, Any]) -> dict[str, str]:
    return {_decode(key): _decode(value) for key, value in raw.items()}


def _lua_path() -> Path:
    filename = "admission_resource_reservation.lua"
    here = Path(__file__).resolve()
    candidates = (
        here.parent.parent / "lua" / filename,
        here.parent.parent.parent.parent.parent
        / "hfa-core"
        / "src"
        / "hfa"
        / "lua"
        / filename,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    for parent in here.parents:
        candidate = parent / "hfa-core" / "src" / "hfa" / "lua" / filename
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"{filename} not found")


@dataclass(frozen=True)
class AdmissionResourceReservationInput:
    operation_id: str
    run_id: str
    tenant_id: str
    estimated_cost_cents: int
    reservation_version: int = _RESERVATION_VERSION

    def __post_init__(self) -> None:
        operation_id = _required_exact_text(self.operation_id, "operation_id")
        if _OPERATION_ID_PATTERN.fullmatch(operation_id) is None:
            raise ValueError(
                "operation_id must match ^run-create:v1:[0-9a-f]{64}$"
            )
        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(
            self,
            "run_id",
            _required_exact_text(self.run_id, "run_id"),
        )
        object.__setattr__(
            self,
            "tenant_id",
            _required_exact_text(self.tenant_id, "tenant_id"),
        )
        object.__setattr__(
            self,
            "estimated_cost_cents",
            _safe_integer(self.estimated_cost_cents, "estimated_cost_cents"),
        )
        version = _safe_integer(
            self.reservation_version,
            "reservation_version",
            minimum=1,
        )
        if version != _RESERVATION_VERSION:
            raise ValueError(f"reservation_version must be {_RESERVATION_VERSION}")
        object.__setattr__(self, "reservation_version", version)

    @property
    def proof_sha256(self) -> str:
        return _component_sha256(
            "admission-resource-reservation",
            str(self.reservation_version),
            self.operation_id,
            self.run_id,
            self.tenant_id,
            str(self.estimated_cost_cents),
        )


@dataclass(frozen=True)
class AdmissionResourceReservationResult:
    status: str
    operation_id: str
    proof_sha256: str
    resource_mutated: bool
    state: str

    @property
    def ok(self) -> bool:
        return self.status in {
            RESERVATION_STATUS_RESERVED,
            RESERVATION_STATUS_ALREADY_RESERVED,
            RESERVATION_STATUS_FINALIZED,
            RESERVATION_STATUS_ALREADY_FINALIZED,
            RESERVATION_STATUS_RELEASED,
            RESERVATION_STATUS_ALREADY_RELEASED,
        }


@dataclass(frozen=True)
class AdmissionResourceReservationReceipt:
    operation_id: str
    run_id: str
    tenant_id: str
    estimated_cost_cents: int
    proof_sha256: str
    reservation_version: int
    state: str
    created_at_ms: int
    finalized_at_ms: int | None
    released_at_ms: int | None


class AdmissionResourceReservationError(RuntimeError):
    """Base error for malformed Redis/Lua reservation results."""


class AdmissionResourceReservationConflictError(
    AdmissionResourceReservationError
):
    """Stored reservation evidence does not match the requested proof."""


class AdmissionResourceReservationManager:
    """Atomic operation-scoped reservation manager.

    The manager is intentionally stateless. Active receipts and all protected
    counters are persistent. Only a RELEASED receipt receives bounded retention.
    """

    def __init__(
        self,
        redis: Any,
        *,
        loader: LuaScriptLoader | Any | None = None,
        released_receipt_ttl_seconds: int = RELEASED_RECEIPT_TTL_SECONDS,
        clock_ms: Any | None = None,
    ) -> None:
        self._redis = redis
        self._loader = loader
        self._released_receipt_ttl_seconds = _safe_integer(
            released_receipt_ttl_seconds,
            "released_receipt_ttl_seconds",
            minimum=1,
        )
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._initialised = False

    @staticmethod
    def concurrent_run_key(tenant_id: str) -> str:
        tenant = _required_exact_text(tenant_id, "tenant_id")
        return f"{RedisKey.PREFIX}:quota:{tenant}:concurrent_runs"

    @staticmethod
    def budget_reserved_key(tenant_id: str) -> str:
        tenant = _required_exact_text(tenant_id, "tenant_id")
        return f"{RedisKey.PREFIX}:quota:{tenant}:budget_reserved_cents"

    @staticmethod
    def reservation_receipt_key(operation_id: str) -> str:
        operation = _required_exact_text(operation_id, "operation_id")
        if _OPERATION_ID_PATTERN.fullmatch(operation) is None:
            raise ValueError(
                "operation_id must match ^run-create:v1:[0-9a-f]{64}$"
            )
        digest = hashlib.sha256(operation.encode("utf-8")).hexdigest()
        return (
            f"{RedisKey.PREFIX}:admission:resource-reservation:v1:{digest}"
        )

    async def initialise(self) -> None:
        if self._initialised:
            return
        if self._loader is None:
            self._loader = LuaScriptLoader(self._redis, _lua_path())
        await self._loader.load()
        self._initialised = True

    def _now_ms(self, value: int | None) -> int:
        actual = self._clock_ms() if value is None else value
        return _safe_integer(actual, "now_ms")

    async def reserve_once(
        self,
        reservation: AdmissionResourceReservationInput,
        *,
        concurrent_run_limit: int | None,
        budget_limit_cents: int | None,
        tenant_inflight_limit: int | None,
        now_ms: int | None = None,
    ) -> AdmissionResourceReservationResult:
        return await self._execute(
            "reserve",
            reservation,
            now_ms=self._now_ms(now_ms),
            concurrent_run_limit=concurrent_run_limit,
            budget_limit_cents=budget_limit_cents,
            tenant_inflight_limit=tenant_inflight_limit,
        )

    async def finalize_once(
        self,
        reservation: AdmissionResourceReservationInput,
        *,
        now_ms: int | None = None,
    ) -> AdmissionResourceReservationResult:
        return await self._execute(
            "finalize",
            reservation,
            now_ms=self._now_ms(now_ms),
            concurrent_run_limit=None,
            budget_limit_cents=None,
            tenant_inflight_limit=None,
        )

    async def release_once(
        self,
        reservation: AdmissionResourceReservationInput,
        *,
        now_ms: int | None = None,
    ) -> AdmissionResourceReservationResult:
        return await self._execute(
            "release",
            reservation,
            now_ms=self._now_ms(now_ms),
            concurrent_run_limit=None,
            budget_limit_cents=None,
            tenant_inflight_limit=None,
        )

    async def _execute(
        self,
        action: str,
        reservation: AdmissionResourceReservationInput,
        *,
        now_ms: int,
        concurrent_run_limit: int | None,
        budget_limit_cents: int | None,
        tenant_inflight_limit: int | None,
    ) -> AdmissionResourceReservationResult:
        if not isinstance(reservation, AdmissionResourceReservationInput):
            raise TypeError(
                "reservation must be AdmissionResourceReservationInput"
            )
        await self.initialise()
        assert self._loader is not None
        raw = await self._loader.run(
            num_keys=4,
            keys=[
                self.reservation_receipt_key(reservation.operation_id),
                self.concurrent_run_key(reservation.tenant_id),
                self.budget_reserved_key(reservation.tenant_id),
                RedisKey.tenant_inflight(reservation.tenant_id),
            ],
            args=[
                action,
                reservation.operation_id,
                reservation.run_id,
                reservation.tenant_id,
                str(reservation.estimated_cost_cents),
                str(reservation.reservation_version),
                reservation.proof_sha256,
                str(now_ms),
                str(_optional_limit(concurrent_run_limit, "concurrent_run_limit")),
                str(_optional_limit(budget_limit_cents, "budget_limit_cents")),
                str(_optional_limit(tenant_inflight_limit, "tenant_inflight_limit")),
                str(self._released_receipt_ttl_seconds),
            ],
        )
        if not isinstance(raw, (list, tuple)) or len(raw) < 3:
            raise AdmissionResourceReservationError(
                f"invalid Lua reservation result: {raw!r}"
            )
        return AdmissionResourceReservationResult(
            status=_decode(raw[0]),
            operation_id=reservation.operation_id,
            proof_sha256=reservation.proof_sha256,
            resource_mutated=_decode(raw[2]) == "1",
            state=_decode(raw[1]),
        )

    async def get_receipt(
        self,
        reservation: AdmissionResourceReservationInput,
    ) -> AdmissionResourceReservationReceipt | None:
        raw = await self._redis.hgetall(
            self.reservation_receipt_key(reservation.operation_id)
        )
        if not raw:
            return None
        values = _decode_hash(raw)
        immutable = {
            "operation_id": reservation.operation_id,
            "run_id": reservation.run_id,
            "tenant_id": reservation.tenant_id,
            "estimated_cost_cents": str(reservation.estimated_cost_cents),
            "reservation_version": str(reservation.reservation_version),
            "proof_sha256": reservation.proof_sha256,
        }
        if any(values.get(key) != value for key, value in immutable.items()):
            raise AdmissionResourceReservationConflictError(
                "stored reservation receipt conflicts with immutable proof"
            )
        try:
            created_at_ms = _safe_integer(
                int(values["created_at_ms"]), "created_at_ms"
            )
            finalized_at_ms = (
                None
                if not values.get("finalized_at_ms")
                else _safe_integer(
                    int(values["finalized_at_ms"]), "finalized_at_ms"
                )
            )
            released_at_ms = (
                None
                if not values.get("released_at_ms")
                else _safe_integer(
                    int(values["released_at_ms"]), "released_at_ms"
                )
            )
        except (KeyError, ValueError) as exc:
            raise AdmissionResourceReservationConflictError(
                "stored reservation lifecycle metadata is invalid"
            ) from exc
        state = values.get("state", "")
        if state not in {
            RESERVATION_STATE_RESERVED,
            RESERVATION_STATE_FINALIZED,
            RESERVATION_STATE_RELEASED,
        }:
            raise AdmissionResourceReservationConflictError(
                "stored reservation state is invalid"
            )
        return AdmissionResourceReservationReceipt(
            operation_id=reservation.operation_id,
            run_id=reservation.run_id,
            tenant_id=reservation.tenant_id,
            estimated_cost_cents=reservation.estimated_cost_cents,
            proof_sha256=reservation.proof_sha256,
            reservation_version=reservation.reservation_version,
            state=state,
            created_at_ms=created_at_ms,
            finalized_at_ms=finalized_at_ms,
            released_at_ms=released_at_ms,
        )

    async def get_resource_snapshot(self, tenant_id: str) -> dict[str, int]:
        keys = (
            self.concurrent_run_key(tenant_id),
            self.budget_reserved_key(tenant_id),
            RedisKey.tenant_inflight(tenant_id),
        )
        if hasattr(self._redis, "mget"):
            values = list(await self._redis.mget(*keys))
        else:
            values = [await self._redis.get(key) for key in keys]

        def decode_counter(value: Any) -> int:
            if value is None:
                return 0
            raw = _decode(value)
            if not raw.isdecimal():
                raise AdmissionResourceReservationConflictError(
                    "resource counter is not a non-negative integer"
                )
            return _safe_integer(int(raw), "resource_counter")

        return {
            "concurrent_runs": decode_counter(values[0]),
            "budget_reserved_cents": decode_counter(values[1]),
            "tenant_inflight": decode_counter(values[2]),
        }
