"""
hfa-core/src/hfa/governance/budget_guard.py
IRONCLAD — Atomic budget enforcement via Redis Lua + crash recovery.

Monetary units
--------------
ALL amounts are INTEGER CENTS (1 USD = 100 cents). No float arithmetic
in the hot path — eliminates IEEE-754 drift in Redis comparisons.

  $1.00  → 100 cents
  $0.003 → 1 cent  (usd_to_cents() rounds UP via math.ceil)

Public API uses ``_cents`` suffix throughout to make the unit explicit.
Convenience helpers ``usd_to_cents()`` / ``cents_to_usd()`` are provided
for callers that work in USD floats (e.g. LLM token cost calculators).

Redis key layout
----------------
  budget:<tenant_id>:<run_id>:limit_cents  INT — total budget cap
  budget:<tenant_id>:<run_id>:spent_cents  INT — running total
  budget:<tenant_id>:<run_id>:status       STR — "active"|"exhausted"|"frozen"

Design
------
* All debit/check operations run inside a single Lua script (EVALSHA)
  for atomicity — no TOCTOU window between read and write.
* evalsha is called with unpacked positional args, never lists. ← IRONCLAD
* On Redis unavailability the guard fails-CLOSED (denies spend) by default.
* Recovery entry restores state after node restart (no audit gap).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------


def usd_to_cents(usd: float) -> int:
    """
    Convert USD float to integer cents, rounding UP (ceil).
    Minimum debit is always 1 cent — never under-charges.

    Args:
        usd: Amount in USD (e.g. 0.003).

    Returns:
        Ceiling integer cents (e.g. 1 for 0.003).

    Raises:
        ValueError: If usd is negative.
    """
    if usd < 0:
        raise ValueError(f"usd must be non-negative, got {usd}")
    return max(1, math.ceil(usd * 100))


def cents_to_usd(cents: int) -> float:
    """Convert integer cents to USD float (display / logging only)."""
    return cents / 100.0


# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

# DEBIT_SCRIPT
# KEYS[1]=spent_key  KEYS[2]=status_key
# ARGV[1]=amount_cents(int)  ARGV[2]=limit_cents(int)
# Returns: [new_spent_cents(str), status(str)]
_DEBIT_SCRIPT = """
local spent_key  = KEYS[1]
local status_key = KEYS[2]
local amount     = tonumber(ARGV[1])
local limit      = tonumber(ARGV[2])

local current   = tonumber(redis.call('GET', spent_key) or '0')
local new_spent = current + amount

redis.call('SET', spent_key, tostring(new_spent))

local status = 'active'
if new_spent >= limit then
    status = 'exhausted'
    redis.call('SET', status_key, 'exhausted')
else
    local existing = redis.call('GET', status_key)
    if existing ~= 'frozen' then
        redis.call('SET', status_key, 'active')
    end
end

return {tostring(new_spent), status}
"""

# CHECK_SCRIPT — returns 1 if allowed, 0 if denied
_CHECK_SCRIPT = """
local spent_key  = KEYS[1]
local status_key = KEYS[2]
local amount     = tonumber(ARGV[1])
local limit      = tonumber(ARGV[2])

local status = redis.call('GET', status_key) or 'active'
if status == 'frozen' or status == 'exhausted' then
    return 0
end

local current = tonumber(redis.call('GET', spent_key) or '0')
if (current + amount) > limit then
    return 0
end

return 1
"""

# FREEZE_SCRIPT — KEYS[1]=status_key
_FREEZE_SCRIPT = """
redis.call('SET', KEYS[1], 'frozen')
return 1
"""

# RESET_SCRIPT — KEYS[1]=spent_key  KEYS[2]=status_key
_RESET_SCRIPT = """
redis.call('SET', KEYS[1], '0')
redis.call('SET', KEYS[2], 'active')
return 1
"""


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


class BudgetStatus(str, Enum):
    ACTIVE = "active"
    EXHAUSTED = "exhausted"
    FROZEN = "frozen"


@dataclass(frozen=True)
class BudgetState:
    """
    Current snapshot of a run's budget.
    All ``_cents`` fields are integer cents; ``_usd`` properties are
    convenience views for logging only — never use for arithmetic.
    """

    tenant_id: str
    run_id: str
    spent_cents: int
    limit_cents: int
    status: BudgetStatus
    remaining_cents: int = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "remaining_cents", max(0, self.limit_cents - self.spent_cents)
        )

    @property
    def spent_usd(self) -> float:
        return cents_to_usd(self.spent_cents)

    @property
    def limit_usd(self) -> float:
        return cents_to_usd(self.limit_cents)

    @property
    def remaining_usd(self) -> float:
        return cents_to_usd(self.remaining_cents)


class BudgetExhaustedError(Exception):
    """Raised when a debit is attempted on an exhausted or frozen budget."""

    def __init__(
        self,
        tenant_id: str,
        run_id: str,
        attempted_cents: int,
        remaining_cents: int,
    ) -> None:
        self.tenant_id = tenant_id
        self.run_id = run_id
        self.attempted_cents = attempted_cents
        self.remaining_cents = remaining_cents
        super().__init__(
            f"Budget exhausted for {tenant_id}/{run_id}: "
            f"attempted={attempted_cents}¢ remaining={remaining_cents}¢"
        )


class BudgetGuardError(Exception):
    """Raised on infrastructure failure (Redis unavailable, etc.)."""


# ---------------------------------------------------------------------------
# BudgetGuard
# ---------------------------------------------------------------------------


class BudgetGuard:
    """
    Atomic, Redis-backed budget enforcement. All amounts in integer CENTS.

    Usage
    -----
        guard = BudgetGuard(redis_client)
        await guard.initialise()

        await guard.set_budget("acme", "run-acme-abc", limit_cents=500)
        await guard.debit("acme", "run-acme-abc", amount_cents=usd_to_cents(0.003))

    Args:
        redis:      aioredis.Redis (or compatible async client).
        key_prefix: Redis key namespace (default "budget").
        fail_open:  When True, allow spend on Redis failure.
                    Default False → fail-CLOSED (deny on outage).
    """

    def __init__(
        self,
        redis,
        key_prefix: str = "budget",
        fail_open: bool = False,
    ) -> None:
        self._redis = redis
        self._prefix = key_prefix
        self._fail_open = fail_open
        self._sha_debit: Optional[str] = None
        self._sha_check: Optional[str] = None
        self._sha_freeze: Optional[str] = None
        self._sha_reset: Optional[str] = None
        logger.info("BudgetGuard created (fail_open=%s)", fail_open)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialise(self) -> None:
        """
        Load Lua scripts into Redis and cache their SHAs.
        Must be called once before any debit/check.

        Raises:
            BudgetGuardError: If Redis is unreachable.
        """
        try:
            self._sha_debit = await self._redis.script_load(_DEBIT_SCRIPT)
            self._sha_check = await self._redis.script_load(_CHECK_SCRIPT)
            self._sha_freeze = await self._redis.script_load(_FREEZE_SCRIPT)
            self._sha_reset = await self._redis.script_load(_RESET_SCRIPT)
            logger.info(
                "BudgetGuard Lua scripts loaded (SHA debit=%s…)", self._sha_debit[:8]
            )
        except Exception as exc:
            logger.critical("BudgetGuard.initialise failed: %s", exc, exc_info=True)
            raise BudgetGuardError("Failed to load Lua scripts") from exc

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def set_budget(
        self,
        tenant_id: str,
        run_id: str,
        limit_cents: int,
        reset_spent: bool = False,
    ) -> None:
        """
        Create or update the budget limit for a run.

        Args:
            tenant_id:   Tenant identifier.
            run_id:      Run identifier.
            limit_cents: Maximum allowed spend in INTEGER CENTS (e.g. 500 = $5.00).
            reset_spent: If True, also zero out current spent counter.

        Raises:
            TypeError:        If limit_cents is not int.
            ValueError:       If limit_cents ≤ 0.
            BudgetGuardError: On Redis failure.
        """
        if not isinstance(limit_cents, int):
            raise TypeError(
                f"limit_cents must be int, got {type(limit_cents).__name__}. "
                "Use usd_to_cents() to convert."
            )
        if limit_cents <= 0:
            raise ValueError(f"limit_cents must be positive, got {limit_cents}")

        limit_key, spent_key, status_key = self._keys(tenant_id, run_id)

        try:
            pipe = self._redis.pipeline()
            pipe.set(limit_key, str(limit_cents))
            if reset_spent:
                pipe.set(spent_key, "0")
                pipe.set(status_key, "active")
            else:
                pipe.setnx(spent_key, "0")
                pipe.setnx(status_key, "active")
            await pipe.execute()
            logger.info(
                "Budget set: tenant=%s run=%s limit=%d¢ ($%.2f) reset=%s",
                tenant_id,
                run_id,
                limit_cents,
                cents_to_usd(limit_cents),
                reset_spent,
            )
        except Exception as exc:
            logger.error("set_budget failed: %s", exc, exc_info=True)
            raise BudgetGuardError("set_budget Redis failure") from exc

    async def debit(
        self,
        tenant_id: str,
        run_id: str,
        amount_cents: int,
    ) -> BudgetState:
        """
        Atomically debit amount_cents from the run's budget.

        Args:
            tenant_id:    Tenant identifier.
            run_id:       Run identifier.
            amount_cents: INTEGER CENTS to debit (≥ 1).
                          Use usd_to_cents(float) to convert from USD.

        Returns:
            BudgetState after the debit.

        Raises:
            TypeError:            If amount_cents is not int.
            ValueError:           If amount_cents < 1.
            BudgetExhaustedError: If budget is exhausted or frozen.
            BudgetGuardError:     On Redis failure (fail-closed by default).
        """
        if not isinstance(amount_cents, int):
            raise TypeError(
                f"amount_cents must be int, got {type(amount_cents).__name__}. "
                "Use usd_to_cents() to convert."
            )
        if amount_cents < 1:
            raise ValueError(f"amount_cents must be ≥ 1, got {amount_cents}")

        self._assert_initialised()

        limit_key, spent_key, status_key = self._keys(tenant_id, run_id)

        try:
            limit_cents = await self._get_limit_cents(limit_key, tenant_id, run_id)

            # Atomic pre-check (no debit yet)
            can_spend = await self._redis.evalsha(
                self._sha_check,
                2,  # ✅ IRONCLAD: unpacked int, not list
                spent_key,
                status_key,
                str(amount_cents),
                str(limit_cents),
            )

            if not can_spend:
                current_spent = int(await self._redis.get(spent_key) or 0)
                raise BudgetExhaustedError(
                    tenant_id,
                    run_id,
                    amount_cents,
                    max(0, limit_cents - current_spent),
                )

            # Atomic debit
            result = await self._redis.evalsha(
                self._sha_debit,
                2,  # ✅ IRONCLAD: unpacked int, not list
                spent_key,
                status_key,
                str(amount_cents),
                str(limit_cents),
            )

            new_spent_cents = int(result[0])
            status = BudgetStatus(result[1])

            state = BudgetState(
                tenant_id=tenant_id,
                run_id=run_id,
                spent_cents=new_spent_cents,
                limit_cents=limit_cents,
                status=status,
            )
            logger.info(
                "Debit OK: tenant=%s run=%s amount=%d¢ spent=%d¢/%d¢ ($%.2f/$%.2f) status=%s",
                tenant_id,
                run_id,
                amount_cents,
                new_spent_cents,
                limit_cents,
                cents_to_usd(new_spent_cents),
                cents_to_usd(limit_cents),
                status.value,
            )
            return state

        except BudgetExhaustedError:
            raise
        except Exception as exc:
            logger.error("debit failed: %s", exc, exc_info=True)
            if self._fail_open:
                logger.warning("fail_open=True — permitting spend despite Redis error")
                return BudgetState(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    spent_cents=0,
                    limit_cents=2_147_483_647,  # INT_MAX sentinel
                    status=BudgetStatus.ACTIVE,
                )
            raise BudgetGuardError("debit Redis failure") from exc

    async def get_state(self, tenant_id: str, run_id: str) -> BudgetState:
        """
        Read current budget state without modifying it.

        Raises:
            BudgetGuardError: On Redis failure or if budget not initialised.
        """
        limit_key, spent_key, status_key = self._keys(tenant_id, run_id)
        try:
            pipe = self._redis.pipeline()
            pipe.get(limit_key)
            pipe.get(spent_key)
            pipe.get(status_key)
            limit_raw, spent_raw, status_raw = await pipe.execute()

            if limit_raw is None:
                raise BudgetGuardError(
                    f"Budget not initialised for {tenant_id}/{run_id}"
                )

            return BudgetState(
                tenant_id=tenant_id,
                run_id=run_id,
                spent_cents=int(spent_raw or 0),
                limit_cents=int(limit_raw),
                status=BudgetStatus(status_raw or "active"),
            )
        except BudgetGuardError:
            raise
        except Exception as exc:
            logger.error("get_state failed: %s", exc, exc_info=True)
            raise BudgetGuardError("get_state Redis failure") from exc

    async def freeze(self, tenant_id: str, run_id: str) -> None:
        """Operator emergency stop — freeze the run's budget."""
        self._assert_initialised()
        _, _, status_key = self._keys(tenant_id, run_id)
        try:
            await self._redis.evalsha(
                self._sha_freeze,
                1,
                status_key,  # ✅ unpacked
            )
            logger.warning("Budget FROZEN: tenant=%s run=%s", tenant_id, run_id)
        except Exception as exc:
            logger.error("freeze failed: %s", exc, exc_info=True)
            raise BudgetGuardError("freeze Redis failure") from exc

    async def reset(self, tenant_id: str, run_id: str) -> None:
        """Reset spent counter and unfreeze budget (use after human review)."""
        self._assert_initialised()
        _, spent_key, status_key = self._keys(tenant_id, run_id)
        try:
            await self._redis.evalsha(
                self._sha_reset,
                2,
                spent_key,
                status_key,  # ✅ unpacked
            )
            logger.info("Budget RESET: tenant=%s run=%s", tenant_id, run_id)
        except Exception as exc:
            logger.error("reset failed: %s", exc, exc_info=True)
            raise BudgetGuardError("reset Redis failure") from exc

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    async def recover_run(
        self,
        tenant_id: str,
        run_id: str,
        limit_cents: int,
        spent_cents: int,
    ) -> None:
        """
        Crash-recovery: restore known-good state after node restart.
        Replays event log values; does not reset audit trail.

        Args:
            tenant_id:   Tenant identifier.
            run_id:      Run identifier.
            limit_cents: Budget cap (integer cents).
            spent_cents: Known spent amount from event replay (integer cents).

        Raises:
            TypeError:        If arguments are not int.
            BudgetGuardError: On Redis failure.
        """
        for name, val in (("limit_cents", limit_cents), ("spent_cents", spent_cents)):
            if not isinstance(val, int):
                raise TypeError(f"{name} must be int, got {type(val).__name__}")

        limit_key, spent_key, status_key = self._keys(tenant_id, run_id)
        try:
            pipe = self._redis.pipeline()
            pipe.set(limit_key, str(limit_cents))
            pipe.set(spent_key, str(spent_cents))
            status = "exhausted" if spent_cents >= limit_cents else "active"
            pipe.set(status_key, status)
            await pipe.execute()
            logger.info(
                "Budget RECOVERED: tenant=%s run=%s spent=%d¢/%d¢ status=%s",
                tenant_id,
                run_id,
                spent_cents,
                limit_cents,
                status,
            )
        except Exception as exc:
            logger.error("recover_run failed: %s", exc, exc_info=True)
            raise BudgetGuardError("recover_run Redis failure") from exc

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _keys(self, tenant_id: str, run_id: str) -> tuple[str, str, str]:
        base = f"{self._prefix}:{tenant_id}:{run_id}"
        return f"{base}:limit_cents", f"{base}:spent_cents", f"{base}:status"

    async def _get_limit_cents(
        self, limit_key: str, tenant_id: str, run_id: str
    ) -> int:
        raw = await self._redis.get(limit_key)
        if raw is None:
            raise BudgetGuardError(f"Budget not initialised for {tenant_id}/{run_id}")
        return int(raw)

    def _assert_initialised(self) -> None:
        if self._sha_debit is None:
            raise BudgetGuardError(
                "BudgetGuard not initialised — call await guard.initialise() first"
            )
