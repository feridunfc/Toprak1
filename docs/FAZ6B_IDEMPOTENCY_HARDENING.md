
# Faz 6B — Idempotency Hardening

## Goal
Prevent duplicate dispatch/completion from corrupting control-plane behavior.

## What this pack adds
- dispatch token store
- completion token store
- first-write-wins semantics
- duplicate detection with existing value visibility
- core + integration tests

## Why it matters
Retries are safe only if duplicates are harmless.
This pack hardens the system against repeated dispatch and completion attempts.
