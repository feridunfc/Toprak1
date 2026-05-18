# HFA V5.3 IRONCLAD — Sprint 1-4 Final Candidate Package

This package is assembled from:
- the latest public repo baseline available in the provided artifacts,
- the most recent uploaded middleware/schema/test files,
- the Sprint 4 readiness report and roadmap.

## Included focus areas
- Sprint 1: schema / LLM / architect-service baseline
- Sprint 2: budget guard, signed ledger, tenant isolation
- Sprint 3: sandbox + core services baseline
- Sprint 4: tenant/rate-limit/ledger patch track + unified test assets

## Important
This is a **final candidate package**, not a guaranteed fully verified release build.
Some repo-wide dependencies and unshared files may still need reconciliation inside your local clone.

## Suggested next commands
1. Extract over a clean clone of the repo.
2. Remove `__pycache__`, `.idea`, local backup files.
3. Install dependencies for `hfa-core` and `hfa-tools`.
4. Run:
   - `pytest hfa-tools/tests/test_budget_guard_atomic.py -v`
   - `pytest hfa-tools/tests/test_tenant_patches.py -v`
   - `pytest hfa-tools/tests/test_sprint2.py -v`

## Canonical test recommendation
Prefer the test files under `hfa-tools/tests/` as the source of truth for Sprint 2/4,
and archive duplicate root-level experimental tests unless they are intentionally maintained.
