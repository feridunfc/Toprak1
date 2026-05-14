"""
hfa-control/src/hfa_control/effect_ledger.py
IRONCLAD Phase 7C-Final — Exactly-once effect admission via atomic SETNX.

Changes vs previous version:
  - EffectReceipt.reason (Optional[str]) added for standardized suppress signals.
  - acquire_effect() TTL is now caller-supplied (no default hardcode).
    Callers read TTL from effect_config.
  - Duplicate receipt preserves previous owner_id and committed_state.
  - reason is set by the caller after acquire_effect() returns, since
    the ledger itself is generic (dispatch vs completion context unknown here).
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Optional
from hfa_control.effect_config import get_completion_effect_ttl

@dataclass
class EffectReceipt:
    """
    Result of an effect admission attempt.

    Fields
    ------
    accepted       : True  → first writer, side effects may proceed.
                     False → duplicate/race, suppress all side effects.
    duplicate      : True when a prior owner already holds the effect key.
    effect_type    : "dispatch" | "complete" (or any caller-defined string).
    token          : Deterministic idempotency token (run+task+attempt+...).
    run_id         : Run identifier.
    owner_id       : Worker/scheduler that owns the effect (prior owner on dup).
    committed_state: Logical state at commit time ("pending", "done", etc.).
    reason         : Standardized suppress reason set by the caller:
                       "duplicate_dispatch_suppressed"
                       "duplicate_completion_suppressed"
                       "stale_owner_fenced"
                     None when accepted=True (no suppression occurred).
    """

    accepted: bool
    duplicate: bool
    effect_type: str
    token: str
    run_id: str
    owner_id: Optional[str] = None
    committed_state: Optional[str] = None
    reason: Optional[str] = None  # Phase 7C-Final: standardized suppress reason

    @classmethod
    def from_json(cls, data: str | bytes) -> "EffectReceipt":
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        raw = json.loads(data)
        # Forward-compat: ignore unknown keys; back-compat: missing reason=None.
        field_names = {
            "accepted", "duplicate", "effect_type", "token",
            "run_id", "owner_id", "committed_state", "reason",
        }
        return cls(**{k: v for k, v in raw.items() if k in field_names})

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)


class EffectLedger:
    """
    Exactly-once effect admission layer using atomic SET NX EX.

    Usage
    -----
    receipt = await ledger.acquire_effect(
        run_id=..., token=..., effect_type=..., owner_id=..., ttl_seconds=...
    )
    if receipt.duplicate:
        receipt.reason = "duplicate_dispatch_suppressed"  # caller sets reason
        return early_no_op(receipt)
    # proceed with side effects
    """

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    @staticmethod
    def effect_key(effect_type: str, run_id: str, token: str) -> str:
        return f"hfa:effect:{effect_type}:{run_id}:{token}"



    async def acquire_effect(
            self,
            run_id: str,
            token: str,
            effect_type: str,
            owner_id: str,
            ttl_seconds: int | None = None,
    ):
        if ttl_seconds is None:
            ttl_seconds = get_completion_effect_ttl()
        """
        Attempt to acquire the effect key via SET NX EX.

        Returns
        -------
        EffectReceipt with accepted=True  → caller may proceed with side effects.
        EffectReceipt with accepted=False → duplicate; caller must no-op.
          Prior owner_id and committed_state are preserved from the stored receipt.
          Caller is responsible for setting receipt.reason before returning to client.
        """
        key = self.effect_key(effect_type, run_id, token)

        proposed = EffectReceipt(
            accepted=True,
            duplicate=False,
            effect_type=effect_type,
            token=token,
            run_id=run_id,
            owner_id=owner_id,
            committed_state="pending",
            reason=None,
        )

        # Bounded loop: handles the narrow race where GET sees None after failed SET NX
        for _ in range(2):
            success = await self._redis.set(key, proposed.to_json(), nx=True, ex=ttl_seconds)
            if success:
                return proposed  # accepted=True — first writer wins

            existing = await self._redis.get(key)
            if existing:
                prior = EffectReceipt.from_json(existing)
                prior.accepted = False
                prior.duplicate = True
                # reason intentionally left None — caller sets context-specific reason
                return prior

        # Fallback: explicit GET after both loops missed
        prior = await self.get_receipt(run_id=run_id, token=token, effect_type=effect_type)
        if prior is not None:
            prior.accepted = False
            prior.duplicate = True
            return prior

        # Last-resort guard: reject and protect system consistency
        return EffectReceipt(
            accepted=False,
            duplicate=True,
            effect_type=effect_type,
            token=token,
            run_id=run_id,
            owner_id=owner_id,
            committed_state="unknown",
            reason=None,
        )

    async def get_receipt(
        self, *, run_id: str, token: str, effect_type: str
    ) -> EffectReceipt | None:
        raw = await self._redis.get(self.effect_key(effect_type, run_id, token))
        if raw is None:
            return None
        return EffectReceipt.from_json(raw)
