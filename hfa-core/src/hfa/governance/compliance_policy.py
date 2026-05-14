"""
hfa-core/src/hfa/governance/compliance_policy.py
IRONCLAD Sprint 4 — CompliancePolicy

Design rules
------------
* Rules are validated at __init__ time — a bad rule list raises ValueError
  immediately, not on the first evaluate() call.
* Catch-all rules (no conditions) are FORBIDDEN — they match every finding
  and bypass the AND logic, making the policy meaningless.
* Rule matching uses strict AND logic: ALL conditions in a rule must match.
* evaluate_all() returns the most restrictive action across all matched rules:
    DENY > HITL > ALLOW
* No print() — structured logging only.
* No asyncio (pure data/logic layer — safe to call from sync or async code).

Conditions supported
---------------------
  pattern   str  — case-insensitive substring match on finding["message"]
  severity  str  — exact match on finding["severity"]
  field     str  — used together with "value": finding[field] == value
                   (both "field" and "value" must be present together)
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Policy action with priority ordering
# ---------------------------------------------------------------------------


class PolicyAction(str, Enum):
    ALLOW = "allow"
    HITL = "hitl"
    DENY = "deny"


# Priority: DENY=3 (most restrictive), HITL=2, ALLOW=1 (least)
_ACTION_PRIORITY: Dict[PolicyAction, int] = {
    PolicyAction.DENY: 3,
    PolicyAction.HITL: 2,
    PolicyAction.ALLOW: 1,
}

# Recognised condition keys — at least one must appear in each rule
_CONDITION_KEYS = frozenset({"pattern", "severity", "field"})


# ---------------------------------------------------------------------------
# Evaluation result
# ---------------------------------------------------------------------------


class EvaluationResult:
    """
    Structured output of evaluate_all().

    Attributes:
        decision: Final PolicyAction (most restrictive across all findings).
        details:  Per-finding detail list with matched action.
        denials:  Findings that triggered DENY.
        hitls:    Findings that triggered HITL.
        summary:  Human-readable verdict string.
    """

    __slots__ = ("decision", "details", "denials", "hitls", "summary")

    def __init__(
        self,
        decision: PolicyAction,
        details: List[Dict[str, Any]],
        denials: List[Dict[str, Any]],
        hitls: List[Dict[str, Any]],
        summary: str,
    ) -> None:
        self.decision = decision
        self.details = details
        self.denials = denials
        self.hitls = hitls
        self.summary = summary

    def __repr__(self) -> str:
        return (
            f"EvaluationResult(decision={self.decision.value!r}, "
            f"denials={len(self.denials)}, hitls={len(self.hitls)})"
        )


# ---------------------------------------------------------------------------
# CompliancePolicy
# ---------------------------------------------------------------------------


class CompliancePolicy:
    """
    Data-driven compliance checker with AND rule semantics.

    Rule format (dict)
    ------------------
    {
        "action":   "deny" | "hitl" | "allow",  # REQUIRED
        "pattern":  "pii",                       # OPTIONAL — substring match on message
        "severity": "critical",                  # OPTIONAL — exact match on severity
        "field":    "category",                  # OPTIONAL — must pair with "value"
        "value":    "financial",                 # OPTIONAL — must pair with "field"
    }

    Rules with no conditions (no "pattern", "severity", or "field" key) are
    FORBIDDEN — they would match every finding, making the policy a no-op.

    Example
    -------
        policy = CompliancePolicy(rules=[
            {"action": "deny",  "pattern": "ssn",      "severity": "critical"},
            {"action": "hitl",  "severity": "high"},
            {"action": "allow", "field": "source", "value": "internal"},
        ])
        result = policy.evaluate_all([
            {"message": "SSN exposed", "severity": "critical"},
        ])
        assert result.decision == PolicyAction.DENY
    """

    def __init__(self, rules: List[Dict[str, Any]]) -> None:
        self._validate(rules)
        self.rules = list(rules)  # defensive copy
        logger.info("CompliancePolicy loaded: %d rules", len(self.rules))

    # ------------------------------------------------------------------
    # Validation (fail fast at init)
    # ------------------------------------------------------------------

    @staticmethod
    def _validate(rules: List[Dict[str, Any]]) -> None:
        """
        Validate all rules at construction time.

        Raises:
            TypeError:  If `rules` is not a list.
            ValueError: On any invalid rule.
        """
        if not isinstance(rules, list):
            raise TypeError(f"rules must be list, got {type(rules).__name__}")

        valid_actions = {a.value for a in PolicyAction}

        for i, rule in enumerate(rules):
            # 1. Must be a dict
            if not isinstance(rule, dict):
                raise ValueError(f"Rule[{i}] must be a dict, got {type(rule).__name__}")

            # 2. "action" key is mandatory
            if "action" not in rule:
                raise ValueError(f"Rule[{i}] missing required key 'action': {rule!r}")

            # 3. action value must be valid
            if rule["action"] not in valid_actions:
                raise ValueError(
                    f"Rule[{i}] invalid action {rule['action']!r}. "
                    f"Must be one of {sorted(valid_actions)}"
                )

            # 4. At least one condition key must be present — no catch-alls
            if not any(k in rule for k in _CONDITION_KEYS):
                raise ValueError(
                    f"Rule[{i}] has no conditions (no 'pattern', 'severity', or "
                    f"'field'). Catch-all rules that match every finding are "
                    f"FORBIDDEN in IRONCLAD policy: {rule!r}"
                )

            # 5. "field" and "value" must appear together
            has_field = "field" in rule
            has_value = "value" in rule
            if has_field != has_value:
                raise ValueError(
                    f"Rule[{i}] has 'field' without 'value' or vice-versa. "
                    f"Both must be present together: {rule!r}"
                )

    # ------------------------------------------------------------------
    # Single-finding evaluation
    # ------------------------------------------------------------------

    def evaluate(self, finding: Dict[str, Any]) -> PolicyAction:
        """
        Evaluate a single finding against all rules.

        Returns:
            Most restrictive PolicyAction that matched, or ALLOW if no match.
        """
        matched = [
            PolicyAction(rule["action"])
            for rule in self.rules
            if self._match(finding, rule)
        ]
        if not matched:
            return PolicyAction.ALLOW
        return max(matched, key=lambda a: _ACTION_PRIORITY[a])

    # ------------------------------------------------------------------
    # Batch evaluation
    # ------------------------------------------------------------------

    def evaluate_all(self, findings: List[Dict[str, Any]]) -> EvaluationResult:
        """
        Evaluate a list of findings and return an aggregated result.

        The final decision is the most restrictive action across all findings:
            DENY > HITL > ALLOW

        Args:
            findings: List of finding dicts from a compliance scan.

        Returns:
            EvaluationResult with decision, per-finding details, and summary.
        """
        if not findings:
            return EvaluationResult(
                decision=PolicyAction.ALLOW,
                details=[],
                denials=[],
                hitls=[],
                summary="ALLOWED (no findings)",
            )

        details: List[Dict[str, Any]] = []
        denials: List[Dict[str, Any]] = []
        hitls: List[Dict[str, Any]] = []
        actions: List[PolicyAction] = []

        for f in findings:
            action = self.evaluate(f)
            actions.append(action)
            details.append({"finding": f, "action": action.value})

            if action == PolicyAction.DENY:
                denials.append(f)
            elif action == PolicyAction.HITL:
                hitls.append(f)

        decision = max(actions, key=lambda a: _ACTION_PRIORITY[a])

        if denials:
            summary = f"DENIED: {len(denials)} critical finding(s)"
        elif hitls:
            summary = f"HITL REQUIRED: {len(hitls)} medium finding(s)"
        else:
            summary = "ALLOWED"

        logger.info(
            "CompliancePolicy.evaluate_all: decision=%s findings=%d "
            "denials=%d hitls=%d",
            decision.value,
            len(findings),
            len(denials),
            len(hitls),
        )

        return EvaluationResult(
            decision=decision,
            details=details,
            denials=denials,
            hitls=hitls,
            summary=summary,
        )

    # ------------------------------------------------------------------
    # Rule matching — AND logic
    # ------------------------------------------------------------------

    @staticmethod
    def _match(finding: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """
        Return True iff ALL conditions defined in `rule` match `finding`.

        AND semantics: every present condition must match.
        An empty condition set never reaches here (forbidden by _validate).

        Conditions:
          pattern   → case-insensitive substring in finding["message"]
          severity  → exact match on finding["severity"]
          field+value → finding[field] == value
        """
        conds: List[bool] = []

        if "pattern" in rule:
            msg = finding.get("message", "")
            conds.append(rule["pattern"].lower() in str(msg).lower())

        if "severity" in rule:
            conds.append(rule["severity"] == finding.get("severity"))

        if "field" in rule and "value" in rule:
            conds.append(finding.get(rule["field"]) == rule["value"])

        # At least one condition must exist (validated at init).
        # Return True only when ALL conditions are met.
        return bool(conds) and all(conds)
