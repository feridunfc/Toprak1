# ADR: IRONCLAD / HFA v3 Constitutional Authority

Status: accepted for Sprint 1 stabilization

## Context

IRONCLAD / HFA already contains event infrastructure, Redis-backed runtime state,
control-plane scheduling, worker execution, recovery, reconciliation, replay, and
semantic sidecar systems.  Sprint 1 does not redesign those systems.  It freezes
the authority rules that future migrations must preserve.

## Decision

The v3 constitution is:

1. The event log is the durable source of truth for authoritative lifecycle
   transitions.
2. Redis-backed runtime state is a projection, cache, lease store, or operational
   index.  It is not the long-term source of truth for replayable lifecycle
   history.
3. Commands request change; commands do not directly author truth.
4. Workers execute effects and report outcomes.  Workers must not be treated as
   the final authority for terminal truth.
5. The control plane owns authoritative scheduling decisions.
6. Replay correctness is mandatory.  Replay must never require live LLM calls.
7. Semantic systems have two modes: advisory and gate.  Advisory may degrade;
   gate must fail closed and be observable.
8. Budget enforcement has one runtime authority source per `BudgetGuard`
   instance: its Redis key namespace.
9. Feature-flag rollback is mandatory for new authority sealing.

## Sprint 1 Event Gate

`IRON_V3_EVENT_GATE` enables the first vertical slice of event-first sealing for
terminal task completion in `StateStore.complete_once`.  When enabled, the
corresponding terminal event must append successfully before Redis lifecycle
state or ownership is mutated.  When disabled, the legacy background event path
is preserved for rollback.

## Non-goals

Sprint 1 does not migrate all Redis state to pure event sourcing, does not
rewrite the scheduler, does not change worker APIs, and does not replace the
existing replay engine.
