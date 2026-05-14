
# Faz 5C — Fairness Hardening

## Goal
Prevent fairness from turning into starvation or permanent punishment.

## What this pack adds
- max inflight per tenant policy
- starvation recovery / vruntime soft reduction
- vruntime floor clamp
- fairness hardening manager
- core + integration tests

## Why it matters
A fairness model that only accumulates penalty can become operationally unfair over time.
This pack bounds punishment and introduces deterministic recovery behavior.
