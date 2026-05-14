
# V23.7 — Recovery Chaos + Ownership Fencing

## Purpose
Hardening layer on top of V23.6B heartbeat + stale recovery.

## What this pack adds
- ownership fencing helper
- chaos-oriented integration tests for stale recovery
- reason-code extensions for ownership/recovery states

## Validated cases
1. fresh heartbeat is not treated as stale
2. owner mismatch heartbeat is rejected
3. duplicate requeue attempt is idempotent
4. terminal task cannot be requeued

## Why this stays in V23
These are still DAG runtime hardening concerns, not V24 product-expansion features.

## V24 starts after this
- task-aware worker consumer
- task-level circuit breaker
- parent output -> child input result passing
- DAG metrics / visualization
