
# Faz 4C — Capability-Aware Fair Reservation Dispatch

## Goal
Combine scheduler-side capability filtering, unified scoring, and authoritative reservation dispatch into one pipeline.

## Pipeline
1. capability filter workers
2. score compatible workers
3. reserve and dispatch best worker
4. worker claim consumes reservation

## What this pack adds
- `SchedulerCapabilityFairDispatcher`
- full pre-dispatch compatible-worker filtering
- unified worker scoring for compatible set
- reservation-aware dispatch integration
- core + integration tests

## Why it matters
This is the first truly production scheduler-shaped decision surface:
capability + fairness + authority in one path.
