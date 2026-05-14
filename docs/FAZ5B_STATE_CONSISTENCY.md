
# Faz 5B — Scheduler State Consistency

## Goal
Prevent the scheduler from trusting stale worker state.

## What this pack adds
- worker heartbeat freshness check
- capacity/load eligibility check
- worker consistency decisions with explicit reasons
- core + integration tests

## Why it matters
A fair and capable scheduler can still make bad decisions if worker state is stale.
This pack makes heartbeat freshness and capacity an explicit precondition for dispatch eligibility.
