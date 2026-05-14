"""
hfa-control/src/hfa_control/scheduler_reasons.py
IRONCLAD Sprint 22 — Standardized scheduler reason taxonomy

Single source of truth for all scheduler reason codes.
Used in: DispatchCommitResult, DispatchAttemptResult, metrics labels, logs.
"""

# Successful outcome
COMMITTED                = "committed"

# Dispatch pacing / quota
DISPATCH_BUDGET_EXHAUSTED = "dispatch_budget_exhausted"

# Queue / tenant selection
NO_ACTIVE_TENANTS         = "no_active_tenants"
TENANT_EMPTY_OR_INVALID   = "tenant_empty_or_invalid"
QUEUE_HEAD_CHANGED        = "queue_head_changed"

# Meta / state
MISSING_META_QUARANTINED  = "missing_meta_quarantined"

# Worker pool
WORKER_POOL_EMPTY         = "worker_pool_empty"

# State machine conflicts (non-requeue: explicit quarantine required)
STATE_CONFLICT            = "state_conflict"
ALREADY_RUNNING           = "already_running"
ILLEGAL_TRANSITION        = "illegal_transition"

# Transient / requeue eligible
INTERNAL_ERROR            = "internal_error"
REQUEUED_TRANSIENT        = "requeued_transient"

# Quarantine outcomes
QUARANTINED_STATE_CONFLICT = "quarantined_state_conflict"
QUARANTINED_ALREADY_RUNNING = "quarantined_already_running"
QUARANTINED_ILLEGAL_TRANSITION = "quarantined_illegal_transition"

# Which reasons should result in explicit quarantine (not silent drop, not requeue)
QUARANTINE_REASONS = frozenset({
    STATE_CONFLICT,
    ALREADY_RUNNING,
    ILLEGAL_TRANSITION,
})

# Which reasons should result in requeue (transient problems)
REQUEUE_REASONS = frozenset({
    INTERNAL_ERROR,
    REQUEUED_TRANSIENT,
})
