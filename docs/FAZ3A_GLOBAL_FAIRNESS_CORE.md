
# Faz 3A — Global Fairness Core

## Goal
Introduce global per-tenant vruntime as the first fairness authority before reconciliation and predictive scheduling.

## What this pack adds
- tenant fairness snapshot model
- normalized runtime by weight
- Redis-backed fairness manager
- per-tenant vruntime and inflight keys
- deterministic fairest-tenant selection

## Rules
- lower vruntime wins
- inflight breaks ties when vruntime is equal
- runtime is normalized by weight
- fairness is deterministic
