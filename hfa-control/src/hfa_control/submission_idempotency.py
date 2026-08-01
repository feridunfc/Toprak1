"""Tenant-scoped idempotent RUN submission gate."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from hfa.config.keys import RedisKey
from hfa.lua.loader import LuaScriptLoader


class SubmissionIdempotencyError(RuntimeError):
    pass


class IdempotencyReservationStatus(str, Enum):
    RESERVED = "RESERVED"
    FINAL_SAME_REQUEST = "FINAL_SAME_REQUEST"
    IN_PROGRESS_SAME_REQUEST = "IN_PROGRESS_SAME_REQUEST"
    DIFFERENT_REQUEST = "DIFFERENT_REQUEST"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"


@dataclass(frozen=True)
class IdempotencyReservation:
    status: IdempotencyReservationStatus
    run_id: str
    task_id: str
    result_json: str = ""


def validate_idempotency_key(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError(
            "Idempotency-Key must be a string"
        )
    if not value:
        raise ValueError(
            "Idempotency-Key must not be empty"
        )
    if value != value.strip():
        raise ValueError(
            "Idempotency-Key must not contain "
            "surrounding whitespace"
        )
    if len(value) > 128:
        raise ValueError(
            "Idempotency-Key must not exceed 128 characters"
        )
    if any(
        ord(character) < 32
        or ord(character) == 127
        for character in value
    ):
        raise ValueError(
            "Idempotency-Key must not contain control characters"
        )
    return value


def canonical_submission_fingerprint(
    request: Any,
) -> str:
    material = {
        "run_shape": str(
            getattr(request, "run_shape", "") or ""
        ),
        "payload": dict(
            getattr(request, "payload", {}) or {}
        ),
        "agent_type": str(
            getattr(request, "agent_type", "") or ""
        ),
        "priority": getattr(request, "priority", None),
        "estimated_cost_cents": getattr(
            request,
            "estimated_cost_cents",
            None,
        ),
        "preferred_region": str(
            getattr(
                request,
                "preferred_region",
                "",
            )
            or ""
        ),
        "preferred_placement": str(
            getattr(
                request,
                "preferred_placement",
                "",
            )
            or ""
        ),
        "required_capabilities": list(
            getattr(
                request,
                "required_capabilities",
                (),
            )
            or ()
        ),
    }
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def idempotency_key_digest(
    idempotency_key: str,
) -> str:
    normalized = validate_idempotency_key(
        idempotency_key
    )
    return hashlib.sha256(
        normalized.encode("utf-8")
    ).hexdigest()


def submission_idempotency_redis_key(
    tenant_id: str,
    idempotency_key: str,
) -> str:
    return RedisKey.submission_idempotency(
        tenant_id,
        idempotency_key_digest(idempotency_key),
    )


def _script_path() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        for relative in (
            "hfa-core/src/hfa/lua/"
            "submission_idempotency.lua",
            "hfa/lua/submission_idempotency.lua",
            "lua/submission_idempotency.lua",
        ):
            candidate = parent / relative
            if candidate.exists():
                return candidate
    raise FileNotFoundError(
        "submission_idempotency.lua not found"
    )


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode(
            "utf-8",
            errors="strict",
        )
    return str(value) if value is not None else ""


class SubmissionIdempotencyStore:
    """Atomic Redis-backed submission reservation/finalization."""

    def __init__(
        self,
        redis: Any,
        *,
        retention_seconds: int,
        loader: Any | None = None,
    ) -> None:
        if (
            type(retention_seconds) is not int
            or retention_seconds <= 0
        ):
            raise ValueError(
                "retention_seconds must be a positive int"
            )
        self._redis = redis
        self._retention_seconds = retention_seconds
        self._loader = (
            loader
            if loader is not None
            else LuaScriptLoader(
                redis,
                _script_path(),
            )
        )
        self._initialised = False

    @property
    def retention_seconds(self) -> int:
        return self._retention_seconds

    async def initialise(self) -> None:
        if self._initialised:
            return
        await self._loader.load()
        self._initialised = True

    async def reserve(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
        request_fingerprint: str,
        run_id: str,
        task_id: str,
        owner_token: str,
        created_at_ms: int,
    ) -> IdempotencyReservation:
        await self.initialise()
        result = await self._loader.run(
            num_keys=1,
            keys=[
                submission_idempotency_redis_key(
                    tenant_id,
                    idempotency_key,
                )
            ],
            args=[
                "reserve",
                tenant_id,
                request_fingerprint,
                run_id,
                task_id,
                owner_token,
                str(created_at_ms),
                str(self._retention_seconds),
                "",
            ],
        )
        return self._reservation(result)

    async def finalize(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
        request_fingerprint: str,
        run_id: str,
        task_id: str,
        owner_token: str,
        result_json: str,
        updated_at_ms: int,
    ) -> None:
        if not result_json:
            raise ValueError(
                "result_json must not be empty"
            )
        await self.initialise()
        result = await self._loader.run(
            num_keys=1,
            keys=[
                submission_idempotency_redis_key(
                    tenant_id,
                    idempotency_key,
                )
            ],
            args=[
                "finalize",
                tenant_id,
                request_fingerprint,
                run_id,
                task_id,
                owner_token,
                str(updated_at_ms),
                str(self._retention_seconds),
                result_json,
            ],
        )
        values = self._values(result)
        if not values or values[0] != "FINALIZED":
            raise SubmissionIdempotencyError(
                "idempotency finalization failed: "
                f"{values!r}"
            )

    async def release(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
        request_fingerprint: str,
        run_id: str,
        task_id: str,
        owner_token: str,
        updated_at_ms: int,
    ) -> None:
        await self.initialise()
        result = await self._loader.run(
            num_keys=1,
            keys=[
                submission_idempotency_redis_key(
                    tenant_id,
                    idempotency_key,
                )
            ],
            args=[
                "release",
                tenant_id,
                request_fingerprint,
                run_id,
                task_id,
                owner_token,
                str(updated_at_ms),
                str(self._retention_seconds),
                "",
            ],
        )
        values = self._values(result)
        if not values or values[0] != "RELEASED":
            raise SubmissionIdempotencyError(
                "idempotency release failed: "
                f"{values!r}"
            )

    @staticmethod
    def _values(result: Any) -> list[str]:
        if not isinstance(result, (list, tuple)):
            raise SubmissionIdempotencyError(
                "idempotency script returned "
                f"non-sequence result: {result!r}"
            )
        return [_decode(value) for value in result]

    @classmethod
    def _reservation(
        cls,
        result: Any,
    ) -> IdempotencyReservation:
        values = cls._values(result)
        if len(values) < 4:
            raise SubmissionIdempotencyError(
                "idempotency reservation returned "
                f"an incomplete result: {values!r}"
            )
        try:
            status = IdempotencyReservationStatus(
                values[0]
            )
        except ValueError as exc:
            raise SubmissionIdempotencyError(
                "idempotency reservation returned "
                f"unknown status: {values[0]!r}"
            ) from exc
        return IdempotencyReservation(
            status=status,
            run_id=values[1],
            task_id=values[2],
            result_json=values[3],
        )
