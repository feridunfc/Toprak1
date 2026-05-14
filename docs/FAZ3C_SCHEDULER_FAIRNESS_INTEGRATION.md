
# Faz 3C — Scheduler Fairness Integration

## Goal
Wire fairness into the scheduler dispatch decision.

## What this pack adds
- `SchedulerFairnessDispatcher`
- fairness-first tenant choice
- reservation-aware dispatch handoff
- deterministic candidate selection within chosen tenant
- core + integration tests

## Decision order
1. pick fairest tenant
2. pick deterministic candidate inside that tenant
3. reserve and dispatch through authoritative reservation path

## Why it matters
Faz 3A measured fairness. Faz 3B corrected fairness drift. Faz 3C finally makes the scheduler act on that fairness signal.
