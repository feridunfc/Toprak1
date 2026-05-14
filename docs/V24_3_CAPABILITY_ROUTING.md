
# V24.3 — Capability-Aware Routing

## Goal
Introduce specialization and qualification into task execution routing.

## What this pack adds
- task capability spec
- worker capability spec
- capability router
- worker-side pre-claim capability filter
- core + integration tests

## Rules
- worker without required capabilities does not claim
- rejection happens before claim, so task state stays scheduled
- capability comparison is normalized and deterministic
