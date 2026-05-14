
# Faz 5D — Trace Persistence + Metrics

## Goal
Persist scheduler decision traces and expose metrics for operational observability.

## What this pack adds
- decision trace stream persistence
- scheduler metric counters
- observable scheduler pipeline wrapper
- core + integration tests

## Why it matters
Explainability without persistence is local. This pack turns decision traces into operational evidence:
- what decision was made
- why it was made
- how often it succeeds or fails
- how often no-compatible-worker situations occur
