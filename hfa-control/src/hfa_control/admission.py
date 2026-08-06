"""
hfa-control/src/hfa_control/admission.py
IRONCLAD Sprint 10/14B — Admission Controller

Gate order (Sprint 5/9 contracts preserved, now centralised here)
-----------------------------------------------------------------
  1. validate_run_id_format                 (Sprint 5)
  2. tenant cross-check                     (Sprint 5)
  3. tenant inflight limit                  (Sprint 14B)
  4. tenant rate limit                      (Sprint 14B)
  5. QuotaManager.check_and_increment_runs  (Sprint 9)
  6. QuotaManager.check_rate_limit          (Sprint 9)
  7. QuotaManager.check_and_reserve_budget  (Sprint 9 v2 — atomic Lua)
  8. Emit RunAdmittedEvent → hfa:stream:control
  9. Set hfa:run:state:{run_id} = 'admitted'

On any gate failure the quota rollback is performed before raising.

IRONCLAD rules
--------------
* No print() — logging only.
* cost_cents: int — no float USD.
* close() not needed (no background tasks).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.events.codec import serialize_event
from hfa.events.schema import RunAdmittedEvent

try:
    from hfa.governance.quota_manager import QuotaManager  # type: ignore
    from hfa_tools.middleware.tenant import TenantFormatError  # type: ignore
    from hfa_tools.middleware.tenant import validate_run_id_format  # type: ignore
except ImportError:
    QuotaManager = None  # type: ignore
    validate_run_id_format = None  # type: ignore
    TenantFormatError = Exception

try:
    from hfa_tools.middleware.tenant import (
        TenantFormatError as CanonicalTenantFormatError,
    )
    from hfa_tools.middleware.tenant import (
        validate_run_id_format as canonical_validate_run_id_format,
    )
except ImportError:
    canonical_validate_run_id_format = None  # type: ignore
    CanonicalTenantFormatError = Exception

try:
    from hfa.obs.tracing import HFATracing, get_tracer  # type: ignore

    _tracer = get_tracer("hfa.admission")
except Exception:
    _tracer = None
    HFATracing = None  # type: ignore

from hfa_control.state_machine import transition_state
from hfa_control.exceptions import (
    AdmissionError,
    BudgetExceededError,
    QuotaExceededError,
    RateLimitedError,
)
from hfa_control.rate_limit import TenantRateLimiter
from hfa_control.tenant_registry import TenantRegistry
from hfa_control.run_create_authority import (
    RUN_CREATE_DUPLICATE_STATUS,
    RUN_CREATE_PROJECTED_STATUS,
    RunCreateAuthorityBinding,
    RunCreateAuthorityError,
    RunCreateResourceError,
)

logger = logging.getLogger(__name__)


def _noop_span():
    """Minimal no-op context manager when OTel is not available."""

    class _Span:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def set_attribute(self, *_):
            pass

    return _Span()


class AdmissionController:
    def __init__(
        self,
        redis,
        config,
        tenant_registry: Optional[TenantRegistry] = None,
        rate_limiter: Optional[TenantRateLimiter] = None,
        audit=None,
        canonical_run_create_binding: bool = False,
        run_create_authority: Optional[RunCreateAuthorityBinding] = None,
    ) -> None:
        self._redis = redis
        self._config = config
        self._quota = QuotaManager(redis) if QuotaManager else None
        self._tenant_registry = tenant_registry
        self._rate_limiter = rate_limiter
        self._audit = audit  # AuditLogger | None
        if type(canonical_run_create_binding) is not bool:
            raise ValueError("canonical_run_create_binding must be a boolean")
        if canonical_run_create_binding and run_create_authority is None:
            raise ValueError(
                "canonical RUN_CREATE binding requires run_create_authority"
            )
        self._canonical_run_create_binding_enabled = (
            canonical_run_create_binding
        )
        self._run_create_authority = run_create_authority

    async def initialise(self) -> None:
        """
        Pre-warm the rate limiter by loading the Lua script into Redis.

        Should be called once at Control Plane startup (in ControlPlaneService.start()).
        Safe to skip — check_and_consume() auto-initialises on first call —
        but calling here avoids a cold-start latency spike on the first admission.
        """
        if self._rate_limiter is not None:
            await self._rate_limiter.initialise()
            logger.info("AdmissionController: rate limiter initialised (EVALSHA ready)")
        if self._canonical_run_create_binding_enabled:
            assert self._run_create_authority is not None
            await self._run_create_authority.initialise()
            logger.info("AdmissionController: canonical RUN_CREATE binding initialised")

    async def _tenant_inflight_allowed(
        self, tenant_id: str
    ) -> tuple[bool, Optional[str]]:
        """
        Returns (allowed, reason).
        """
        if not self._tenant_registry:
            return True, None

        config = await self._tenant_registry.get_config(tenant_id)
        if config.max_inflight_runs is None:
            return True, None

        inflight = await self._tenant_registry.get_inflight(tenant_id)
        if inflight >= config.max_inflight_runs:
            return False, "tenant_inflight_limit_exceeded"

        return True, None

    async def _tenant_rate_allowed(self, tenant_id: str) -> tuple[bool, Optional[str]]:
        """
        Returns (allowed, reason).
        """
        if not self._tenant_registry or not self._rate_limiter:
            return True, None

        config = await self._tenant_registry.get_config(tenant_id)
        if config.max_runs_per_second is None:
            return True, None

        allowed = await self._rate_limiter.check_and_consume(
            tenant_id,
            config.max_runs_per_second,
        )
        if not allowed:
            return False, "tenant_rate_limit_exceeded"

        return True, None

    async def _reject_run(self, request, reason: str) -> None:
        """
        Mark run rejected with explicit reason.
        No event emitted - run stops here.
        """
        run_id = request.run_id

        # CAS: reject only if not already in a state
        _rjr = await transition_state(
            self._redis,
            run_id,
            "rejected",
            state_key=RedisKey.run_state(run_id),
            state_ttl=RedisTTL.RUN_STATE,
            expected_state=None,
        )
        await self._redis.hset(
            RedisKey.run_meta(run_id),
            mapping={
                "run_id": run_id,
                "tenant_id": request.tenant_id,
                "agent_type": request.agent_type,
                "rejection_reason": reason,
                "rejected_at": str(time.time()),
            },
        )
        await self._redis.expire(
            RedisKey.run_meta(run_id),
            RedisTTL.RUN_META,
        )

        logger.info(
            "Run rejected: run=%s tenant=%s reason=%s",
            run_id,
            request.tenant_id,
            reason,
        )
        if self._audit:
            await self._audit.rejected(
                run_id=run_id,
                tenant_id=request.tenant_id,
                reason=reason,
            )

    async def admit(self, request) -> str:
        """Dispatch to the default legacy path or flagged canonical RUN_CREATE."""
        if not self._canonical_run_create_binding_enabled:
            return await self._admit_legacy(request)
        return await self._admit_canonical(request)

    async def _validate_request_identity(self, request) -> None:
        if canonical_validate_run_id_format:
            try:
                ext_tenant, _ = canonical_validate_run_id_format(request.run_id)
                if ext_tenant != request.tenant_id:
                    raise AdmissionError(
                        f"run_id tenant mismatch: run_id encodes {ext_tenant!r}, "
                        f"header says {request.tenant_id!r}"
                    )
            except CanonicalTenantFormatError as exc:
                raise AdmissionError(str(exc)) from exc

    async def _admit_canonical(self, request) -> str:
        assert self._run_create_authority is not None
        span = (
            _tracer.start_as_current_span("hfa.admission.admit.canonical")
            if _tracer
            else _noop_span()
        )
        with span as sp:
            _set_attr(sp, "hfa.run_id", request.run_id)
            _set_attr(sp, "hfa.tenant_id", request.tenant_id)
            _set_attr(sp, "hfa.agent_type", request.agent_type)
            try:
                await self._validate_request_identity(request)

                tenant_config = (
                    await self._tenant_registry.get_config(request.tenant_id)
                    if self._tenant_registry is not None
                    else None
                )

                # Request-attempt rate accounting is intentionally not refundable.
                if (
                    tenant_config is not None
                    and tenant_config.max_runs_per_second is not None
                    and self._rate_limiter is not None
                ):
                    allowed = await self._rate_limiter.check_and_consume(
                        request.tenant_id,
                        tenant_config.max_runs_per_second,
                    )
                    if not allowed:
                        await self._reject_run(
                            request,
                            "tenant_rate_limit_exceeded",
                        )
                        raise RateLimitedError(
                            f"Tenant rate limit exceeded: {request.tenant_id!r}"
                        )

                try:
                    result = await self._run_create_authority.admit(
                        request,
                        tenant_inflight_limit=(
                            None
                            if tenant_config is None
                            else tenant_config.max_inflight_runs
                        ),
                        # The exact parent has no canonical provider for these
                        # two limits. Accounting remains active; enforcement is
                        # intentionally unavailable rather than invented.
                        concurrent_run_limit=None,
                        budget_limit_cents=None,
                    )
                except RunCreateResourceError as exc:
                    if exc.resource_status == "tenant_inflight_exceeded":
                        await self._reject_run(
                            request,
                            "tenant_inflight_limit_exceeded",
                        )
                        raise QuotaExceededError(
                            f"Tenant inflight limit exceeded: {request.tenant_id!r}"
                        ) from exc
                    if exc.resource_status == "concurrent_run_quota_exceeded":
                        await self._reject_run(request, "system_quota_exceeded")
                        raise QuotaExceededError(
                            f"Concurrent run limit exceeded: {request.tenant_id!r}"
                        ) from exc
                    if exc.resource_status == "budget_exceeded":
                        await self._reject_run(request, "budget_exceeded")
                        raise BudgetExceededError(
                            f"Budget exceeded: {request.tenant_id!r}"
                        ) from exc
                    raise

                if result.status not in {
                    RUN_CREATE_PROJECTED_STATUS,
                    RUN_CREATE_DUPLICATE_STATUS,
                }:
                    raise RunCreateAuthorityError(
                        status=result.status,
                        detail="RUN_CREATE did not complete admitted projection",
                        canonical_commit_durable=True,
                    )

                logger.info(
                    "Canonical RUN_CREATE admitted: run=%s tenant=%s status=%s",
                    request.run_id,
                    request.tenant_id,
                    result.status,
                )
                if self._audit and result.first_projection:
                    try:
                        await self._audit.admitted(
                            run_id=request.run_id,
                            tenant_id=request.tenant_id,
                            agent_type=request.agent_type,
                            priority=request.priority,
                            estimated_cost_cents=int(
                                getattr(request, "estimated_cost_cents", 0) or 0
                            ),
                        )
                    except Exception as audit_exc:
                        # Canonical authority, resource finalization and the
                        # admitted projection are already durable. Audit failure
                        # must not misreport the RUN as rejected or trigger any
                        # compensation path.
                        logger.error(
                            "Canonical RUN_CREATE audit write failed: run=%s %s",
                            request.run_id,
                            audit_exc,
                            exc_info=True,
                        )
                _set_attr(sp, "hfa.admitted", "true")
                return result.run_id
            except (AdmissionError, RunCreateAuthorityError):
                _set_attr(sp, "hfa.admitted", "false")
                raise
            except Exception as exc:
                _set_attr(sp, "hfa.admitted", "false")
                logger.error(
                    "AdmissionController canonical RUN_CREATE error: run=%s %s",
                    request.run_id,
                    exc,
                    exc_info=True,
                )
                raise

    async def _admit_legacy(self, request) -> str:
        """
        Admit a RunRequest through the exact legacy path.
        Returns run_id on success.
        Raises AdmissionError subclass on rejection.

        request must expose:
          .run_id, .tenant_id, .agent_type, .priority,
          .payload (dict), .estimated_cost_cents (int),
          .preferred_region (str, optional),
          .preferred_placement (str, optional)
        """
        incremented_tenant_inflight = False
        incremented_system_runs = False

        span = (
            _tracer.start_as_current_span("hfa.admission.admit")
            if _tracer
            else _noop_span()
        )
        with span as sp:
            _set_attr(sp, "hfa.run_id", request.run_id)
            _set_attr(sp, "hfa.tenant_id", request.tenant_id)
            _set_attr(sp, "hfa.agent_type", request.agent_type)

            try:
                # Gate 1+2: format + tenant
                if validate_run_id_format:
                    try:
                        ext_tenant, _ = validate_run_id_format(request.run_id)
                        if ext_tenant != request.tenant_id:
                            raise AdmissionError(
                                f"run_id tenant mismatch: "
                                f"run_id encodes {ext_tenant!r}, "
                                f"header says {request.tenant_id!r}"
                            )
                    except TenantFormatError as exc:
                        raise AdmissionError(str(exc)) from exc

                # Gate 3: tenant inflight limit
                allowed, reason = await self._tenant_inflight_allowed(request.tenant_id)
                if not allowed:
                    await self._reject_run(request, reason or "tenant_rejected")
                    raise QuotaExceededError(
                        f"Tenant inflight limit exceeded: {request.tenant_id!r}"
                    )

                # Gate 4: tenant rate limit
                allowed, reason = await self._tenant_rate_allowed(request.tenant_id)
                if not allowed:
                    await self._reject_run(request, reason or "tenant_rejected")
                    raise RateLimitedError(
                        f"Tenant rate limit exceeded: {request.tenant_id!r}"
                    )

                # Gate 5: concurrent run quota
                if self._quota:
                    if not await self._quota.check_and_increment_runs(
                        request.tenant_id
                    ):
                        await self._reject_run(request, "system_quota_exceeded")
                        raise QuotaExceededError(
                            f"Concurrent run limit exceeded: {request.tenant_id!r}"
                        )
                    incremented_system_runs = True

                # Gate 6: system rate limit
                if self._quota:
                    if not await self._quota.check_rate_limit(request.tenant_id):
                        await self._quota.decrement_runs(request.tenant_id)
                        incremented_system_runs = False
                        await self._reject_run(request, "system_rate_limit_exceeded")
                        raise RateLimitedError(
                            f"RPM limit exceeded: {request.tenant_id!r}"
                        )

                # Gate 7: atomic budget reserve
                estimated = int(getattr(request, "estimated_cost_cents", 0))
                if self._quota and estimated > 0:
                    if not await self._quota.check_and_reserve_budget(
                        request.tenant_id, estimated
                    ):
                        if incremented_system_runs:
                            await self._quota.decrement_runs(request.tenant_id)
                            incremented_system_runs = False
                        await self._reject_run(request, "budget_exceeded")
                        raise BudgetExceededError(
                            f"Budget exceeded: {request.tenant_id!r}"
                        )
                else:
                    estimated = int(getattr(request, "estimated_cost_cents", 0))

                # All gates passed → tenant inflight commit
                if self._tenant_registry:
                    await self._tenant_registry.increment_inflight(request.tenant_id)
                    incremented_tenant_inflight = True

                evt = RunAdmittedEvent(
                    run_id=request.run_id,
                    tenant_id=request.tenant_id,
                    agent_type=request.agent_type,
                    priority=request.priority,
                    payload=(
                        request.payload
                        if isinstance(request.payload, dict)
                        else vars(request.payload)
                    ),
                    estimated_cost_cents=estimated,
                    preferred_region=getattr(request, "preferred_region", ""),
                    preferred_placement=getattr(
                        request, "preferred_placement", "LEAST_LOADED"
                    ),
                )

                try:
                    if HFATracing:
                        HFATracing.inject_trace_context(evt)  # type: ignore[attr-defined]
                except Exception:
                    pass

                # Gate first: state must be written before event is emitted
                # Prevents duplicate-admission race: two concurrent requests
                # for the same run_id — only the first gets admitted.
                _adm_ok = await transition_state(
                    self._redis,
                    request.run_id,
                    "admitted",
                    state_key=RedisKey.run_state(request.run_id),
                    state_ttl=RedisTTL.RUN_STATE,
                    expected_state=None,
                )
                if not _adm_ok:
                    logger.warning(
                        "Admission duplicate ignored: run=%s state already set",
                        request.run_id,
                    )
                    return False

                # Emit event only after state is committed
                await self._redis.xadd(
                    self._config.control_stream,
                    serialize_event(evt),
                    maxlen=100_000,
                    approximate=True,
                )

                logger.info(
                    "Admitted: run=%s tenant=%s agent=%s priority=%d",
                    request.run_id,
                    request.tenant_id,
                    request.agent_type,
                    request.priority,
                )
                if self._audit:
                    await self._audit.admitted(
                        run_id=request.run_id,
                        tenant_id=request.tenant_id,
                        agent_type=request.agent_type,
                        priority=request.priority,
                        estimated_cost_cents=int(
                            getattr(request, "estimated_cost_cents", 0) or 0
                        ),
                    )
                _set_attr(sp, "hfa.admitted", "true")
                return request.run_id

            except AdmissionError:
                _set_attr(sp, "hfa.admitted", "false")
                raise
            except Exception as exc:
                _set_attr(sp, "hfa.admitted", "false")

                if incremented_tenant_inflight and self._tenant_registry:
                    await self._tenant_registry.decrement_inflight(request.tenant_id)

                if incremented_system_runs and self._quota:
                    await self._quota.decrement_runs(request.tenant_id)

                logger.error(
                    "AdmissionController.admit unexpected error: run=%s %s",
                    request.run_id,
                    exc,
                    exc_info=True,
                )
                raise


def _set_attr(span, key: str, value: str) -> None:
    try:
        span.set_attribute(key, value)
    except Exception:
        pass
