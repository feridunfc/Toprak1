
# Faz 6A — Redis / Lua Failure Semantics

## Goal
Standardize retry-vs-fail-fast behavior for Redis and Lua calls.

## What this pack adds
- error classification (`transient`, `terminal`, `unknown`)
- distributed error wrapper
- retry policy with exponential backoff
- additive `StateStore` examples for protected Redis/Lua calls
- core + integration tests

## Why it matters
A production control plane must not treat:
- temporary timeouts
- terminal data/script errors
- unknown failures
as the same class of problem.
