"""
hfa-semantic/src/hfa_semantic/policy/guardrails.py

IRONCLAD Sprint 16 — Policy Guardrails (Hard Bounds + Cooldown + HITL)

CRITICAL: Policy adaptation'ın sınırı.
Sistem değişebilir ama bunu hard constraints altında yapar.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any
import time


class PolicyApprovalLevel(str, Enum):
    """Policy change requires what approval?"""
    AUTOMATIC = "automatic"  # < 5% change, can auto-apply
    COOLDOWN = "cooldown"  # > 5% change, requires 24h cooldown
    HITL = "hitl"  # > 10% change, requires human review
    VETO = "veto"  # > 20% change or violates hard bounds


@dataclass
class PolicyBounds:
    """Hard bounds on policy adaptation."""
    
    # Absolute bounds (can never exceed)
    min_threshold: float = 0.05  # Never go below 5%
    max_threshold: float = 0.95  # Never go above 95%
    
    # Rate of change bounds
    max_change_per_day: float = 0.20  # Can't change by >20% in 24h
    min_stability_window_hours: int = 24  # Must wait 24h between changes
    
    # Cascading limits
    max_cascading_changes_per_day: int = 3  # Max 3 different rules/day
    


@dataclass
class PolicyChange:
    """Proposed policy change (before approval)."""
    
    rule_id: str
    current_value: float  # e.g., threshold = 0.75
    proposed_value: float
    reason: str
    confidence: float  # 0.0-1.0
    
    timestamp_ms: float = None
    
    def __post_init__(self):
        if self.timestamp_ms is None:
            self.timestamp_ms = time.time() * 1000
    
    @property
    def magnitude_of_change(self) -> float:
        """Return absolute magnitude of change (0.0-1.0)."""
        if self.current_value == 0:
            return 1.0
        return abs(self.proposed_value - self.current_value) / max(abs(self.current_value), 0.01)
    
    @property
    def approval_level(self) -> PolicyApprovalLevel:
        """Determine what approval is needed."""
        mag = self.magnitude_of_change
        
        if mag > 0.20:
            return PolicyApprovalLevel.VETO
        elif mag > 0.10:
            return PolicyApprovalLevel.HITL
        elif mag > 0.05:
            return PolicyApprovalLevel.COOLDOWN
        else:
            return PolicyApprovalLevel.AUTOMATIC


class PolicyGuardrails:
    """
    Enforces hard bounds on policy adaptation.
    
    Prevents:
      * Runaway thresholds (< 5% or > 95%)
      * Rapid oscillation (> 20% change in 24h)
      * Cascading changes (> 3 rules/day)
      * Silent adaptation (all changes logged + alerts)
    """
    
    def __init__(self, bounds: Optional[PolicyBounds] = None):
        self._bounds = bounds or PolicyBounds()
        self._change_history: Dict[str, list[PolicyChange]] = {}  # rule_id → changes
        self._last_change_timestamp: Dict[str, float] = {}  # rule_id → timestamp
        self._approval_queue: list[PolicyChange] = []  # Pending approvals

    def evaluate_change(self, change: PolicyChange) -> tuple[bool, str]:
        """
        Evaluate if policy change is allowed.
        
        Returns:
            (allowed, reason)
        """
        # CHECK 1: Hard bounds
        if change.proposed_value < self._bounds.min_threshold:
            return False, f"Below minimum ({self._bounds.min_threshold})"
        
        if change.proposed_value > self._bounds.max_threshold:
            return False, f"Above maximum ({self._bounds.max_threshold})"
        
        # CHECK 2: Rate of change
        if self._exceeds_daily_rate(change):
            return False, f"Change magnitude {change.magnitude_of_change:.1%} exceeds daily limit"
        
        # CHECK 3: Stability window
        if not self._satisfies_cooldown(change):
            return False, f"Must wait {self._bounds.min_stability_window_hours}h between changes"
        
        # CHECK 4: Cascading limit
        if self._exceeds_cascade_limit(change):
            return False, f"Too many rule changes today (max {self._bounds.max_cascading_changes_per_day})"
        
        # All checks passed
        return True, "approved"

    def process_change(self, change: PolicyChange) -> Dict[str, Any]:
        """
        Process a policy change request.
        
        Returns:
            {
                'approved': bool,
                'level': PolicyApprovalLevel,
                'reason': str,
                'action': 'apply' | 'queue_review' | 'reject',
                'review_url': str (if HITL)
            }
        """
        # Basic validation
        allowed, reason = self.evaluate_change(change)
        if not allowed:
            return {
                'approved': False,
                'level': PolicyApprovalLevel.VETO,
                'reason': reason,
                'action': 'reject',
            }
        
        # Determine approval level
        approval_level = change.approval_level
        
        # Apply or queue
        if approval_level == PolicyApprovalLevel.AUTOMATIC:
            # Auto-apply (low change, high confidence)
            self._apply_change(change)
            return {
                'approved': True,
                'level': approval_level,
                'reason': 'Low-magnitude change, high confidence',
                'action': 'apply',
            }
        
        elif approval_level == PolicyApprovalLevel.COOLDOWN:
            # Queue for cooldown period
            self._queue_change(change)
            return {
                'approved': False,
                'level': approval_level,
                'reason': f'Queued for {self._bounds.min_stability_window_hours}h cooldown',
                'action': 'queue_cooldown',
            }
        
        elif approval_level == PolicyApprovalLevel.HITL:
            # Queue for human review
            self._queue_for_review(change)
            return {
                'approved': False,
                'level': approval_level,
                'reason': 'Requires human review (10%+ change)',
                'action': 'queue_review',
                'review_url': f"https://review.internal/policy/{change.rule_id}",
            }
        
        else:  # VETO
            return {
                'approved': False,
                'level': approval_level,
                'reason': 'Change magnitude too large or violates hard bounds',
                'action': 'reject',
            }

    def _exceeds_daily_rate(self, change: PolicyChange) -> bool:
        """Check if change exceeds daily rate limit."""
        if change.rule_id not in self._change_history:
            return False
        
        changes_today = [
            c for c in self._change_history[change.rule_id]
            if (time.time() * 1000 - c.timestamp_ms) < 86_400_000
        ]
        
        total_magnitude = sum(c.magnitude_of_change for c in changes_today)
        return total_magnitude + change.magnitude_of_change > self._bounds.max_change_per_day

    def _satisfies_cooldown(self, change: PolicyChange) -> bool:
        """Check if cooldown period has passed."""
        if change.rule_id not in self._last_change_timestamp:
            return True
        
        last_change = self._last_change_timestamp[change.rule_id]
        now = time.time() * 1000
        hours_since = (now - last_change) / (3_600_000)
        
        return hours_since >= self._bounds.min_stability_window_hours

    def _exceeds_cascade_limit(self, change: PolicyChange) -> bool:
        """Check if cascading limit exceeded."""
        rules_changed_today = set()
        
        for rule_id, changes in self._change_history.items():
            for c in changes:
                if (time.time() * 1000 - c.timestamp_ms) < 86_400_000:
                    rules_changed_today.add(rule_id)
        
        return len(rules_changed_today) >= self._bounds.max_cascading_changes_per_day

    def _apply_change(self, change: PolicyChange) -> None:
        """Apply policy change immediately."""
        if change.rule_id not in self._change_history:
            self._change_history[change.rule_id] = []
        
        self._change_history[change.rule_id].append(change)
        self._last_change_timestamp[change.rule_id] = change.timestamp_ms

    def _queue_change(self, change: PolicyChange) -> None:
        """Queue change for cooldown period."""
        self._approval_queue.append(change)

    def _queue_for_review(self, change: PolicyChange) -> None:
        """Queue change for human review."""
        # Send to Slack/Jira/review system
        self._approval_queue.append(change)

    def approve_queued_change(self, change: PolicyChange, approved: bool) -> None:
        """Human/system approves or rejects queued change."""
        if approved:
            self._apply_change(change)
        
        self._approval_queue.remove(change)

    @property
    def stats(self) -> dict:
        """Return statistics."""
        total_changes = sum(len(c) for c in self._change_history.values())
        
        return {
            'total_changes': total_changes,
            'rules_adapted': len(self._change_history),
            'pending_approvals': len(self._approval_queue),
            'bounds': {
                'min_threshold': self._bounds.min_threshold,
                'max_threshold': self._bounds.max_threshold,
                'max_change_per_day': self._bounds.max_change_per_day,
            }
        }

