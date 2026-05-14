"""
hfa-core/src/hfa/schemas/supervisor.py
IRONCLAD — Supervisor policy injection schemas.

These models extend the base SupervisorRequest / SupervisorDecision
defined in agent.py with full policy detail needed for Sprint 2:
  * BudgetPolicy   — per-run spend limits + alert thresholds
  * ComplianceRule — allow / hitl / deny with AND-logic conditions
  * RoutingPolicy  — where to send the request post-validation
  * SupervisorPolicy — composite wrapper injected into every run
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Policy enums
# ---------------------------------------------------------------------------


class ComplianceAction(str, Enum):
    ALLOW = "allow"
    HITL = "hitl"  # human-in-the-loop review required
    DENY = "deny"


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# ---------------------------------------------------------------------------
# BudgetPolicy
# ---------------------------------------------------------------------------


class BudgetPolicy(BaseModel):
    """
    Per-run token and cost budget limits.

    All threshold fields are fractions of the limit (0.0–1.0).
    E.g. alert_at_fraction=0.8 → alert when 80 % of limit spent.
    """

    limit_usd: float = Field(
        ...,
        gt=0,
        le=100_000.0,
        description="Maximum allowed spend in USD for the run.",
    )
    alert_at_fraction: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Emit warning when spent/limit exceeds this fraction.",
    )
    hard_stop_at_fraction: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Freeze run when spent/limit exceeds this fraction. "
        "Normally 1.0 (at exhaustion); set lower for safety margin.",
    )
    alert_severity: AlertSeverity = Field(
        default=AlertSeverity.WARNING,
        description="Severity of budget-threshold alerts.",
    )

    @model_validator(mode="after")
    def alert_before_hard_stop(self) -> "BudgetPolicy":
        if self.alert_at_fraction > self.hard_stop_at_fraction:
            raise ValueError("alert_at_fraction must be ≤ hard_stop_at_fraction")
        return self


# ---------------------------------------------------------------------------
# ComplianceRule
# ---------------------------------------------------------------------------


class ComplianceCondition(BaseModel):
    """
    A single condition that must be true (AND logic with siblings).

    field_path:  Dot-separated path into the request payload
                 (e.g. "context.source", "requirement").
    operator:    One of: "contains", "not_contains", "equals",
                         "not_equals", "matches_regex".
    value:       Expected value (string for comparison, regex for matches_regex).
    """

    field_path: str = Field(..., min_length=1, max_length=200)
    operator: str = Field(
        ...,
        pattern="^(contains|not_contains|equals|not_equals|matches_regex)$",
    )
    value: str = Field(..., min_length=1, max_length=500)


class ComplianceRule(BaseModel):
    """
    A single compliance rule.

    When ALL conditions evaluate to True (AND logic), action is taken.
    Rules are evaluated in priority order (lower number = higher priority).

    Args:
        rule_id:     Unique identifier.
        description: Human-readable explanation (required for audit).
        action:      What to do when rule fires.
        conditions:  All conditions must be met for rule to fire.
        priority:    Evaluation order (1 = highest priority).
        enabled:     Disabled rules are skipped silently.
    """

    rule_id: str = Field(..., min_length=1, max_length=100)
    description: str = Field(..., min_length=10, max_length=500)
    action: ComplianceAction
    conditions: list[ComplianceCondition] = Field(
        ...,
        min_length=1,
        description="All conditions must match (AND logic).",
    )
    priority: int = Field(default=100, ge=1, le=9999)
    enabled: bool = True


# ---------------------------------------------------------------------------
# RoutingPolicy
# ---------------------------------------------------------------------------


class RoutingPolicy(BaseModel):
    """
    Determines which agent receives the request after supervisor validation.

    default_agent:    Fallback agent when no routing rule matches.
    max_retries:      Max re-queuing attempts on transient failure.
    timeout_seconds:  Per-agent call timeout enforced by supervisor.
    """

    default_agent: str = Field(
        ...,
        pattern="^(architect|researcher|coder|tester|debugger|prompt_engineer)$",
    )
    max_retries: int = Field(default=2, ge=0, le=10)
    timeout_seconds: int = Field(default=120, ge=1, le=3600)
    priority_boost_on_retry: bool = Field(
        default=False,
        description="When True, retried tasks are front-queued.",
    )


# ---------------------------------------------------------------------------
# SupervisorPolicy — composite, injected into every run
# ---------------------------------------------------------------------------


class SupervisorPolicy(BaseModel):
    """
    Composite policy object injected by the supervisor middleware
    into every run context before agent dispatch.

    Sprint 2 fields; Sprint 4 adds CompliancePolicy (AND-logic engine).

    Args:
        policy_id:   Unique policy identifier (for versioned policy store).
        version:     SemVer string.
        budget:      Budget limits and alert thresholds.
        routing:     Target agent and retry/timeout config.
        rules:       Ordered list of compliance rules.
        audit_trail: Whether to append each decision to the SignedLedger.
    """

    policy_id: str = Field(
        ...,
        min_length=3,
        max_length=100,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]{2,98}[a-zA-Z0-9]$",
    )
    version: str = Field(
        default="1.0.0",
        pattern=r"^\d+\.\d+\.\d+$",
    )
    budget: BudgetPolicy
    routing: RoutingPolicy
    rules: list[ComplianceRule] = Field(default_factory=list)
    audit_trail: bool = Field(
        default=True,
        description="Append supervisor decisions to the SignedLedger.",
    )

    def sorted_rules(self) -> list[ComplianceRule]:
        """Return enabled rules sorted by ascending priority."""
        return sorted(
            (r for r in self.rules if r.enabled),
            key=lambda r: r.priority,
        )


# ---------------------------------------------------------------------------
# PolicyInjectionResult — returned by supervisor middleware
# ---------------------------------------------------------------------------


class PolicyInjectionResult(BaseModel):
    """
    Result of supervisor policy evaluation for a single request.

    Attributes:
        action:        Final compliance action (allow / hitl / deny).
        policy_id:     Which policy was applied.
        fired_rule_id: ID of the rule that triggered the action,
                       None if no rule fired (default allow).
        reasoning:     Human-readable explanation for audit.
        budget_state:  Serialised BudgetState snapshot at decision time.
    """

    action: ComplianceAction
    policy_id: str
    fired_rule_id: Optional[str] = None
    reasoning: str = Field(..., min_length=10, max_length=1000)
    budget_remaining_usd: Annotated[float, Field(ge=0.0)] = 0.0
    requires_human_review: bool = False

    @model_validator(mode="after")
    def sync_hitl_flag(self) -> "PolicyInjectionResult":
        if self.action == ComplianceAction.HITL:
            object.__setattr__(self, "requires_human_review", True)
        return self
