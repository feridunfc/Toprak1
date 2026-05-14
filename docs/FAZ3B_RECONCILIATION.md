
# Faz 3B — Reconciliation

## Goal
Correct tenant inflight/accounting drift using the running task set as ground truth.

## Why now
The external audit correctly highlighted that fairness without drift correction can become mathematically wrong over time, especially after crashes, missed decrements, or stale state.

## What this pack adds
- `ReconciliationManager`
- per-tenant inflight reconciliation
- correction reports
- core + integration tests

## Ground truth
- `hfa:dag:tenant:<tenant_id>:running` zset cardinality

## Corrected state
- `hfa:dag:tenant:<tenant_id>:inflight`
