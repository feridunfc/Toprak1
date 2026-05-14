
# Faz 6C — Chaos Hardening

## Goal
Move from correctness-under-tests to resilience-under-faults.

## What this pack adds
- chaos fixtures
- transient Redis blackout test
- duplicate dispatch/completion storm tests
- zombie completion test
- worker assassination / reconciliation convergence test

## Why it matters
Production failures do not arrive one at a time and they do not fail politely.
This phase validates that:
- retries do not corrupt state
- duplicates remain harmless
- stale actors cannot overwrite good state
- reconciliation converges after crash-style state loss

## Run
```bash
pytest tests/chaos/ -vv
```
