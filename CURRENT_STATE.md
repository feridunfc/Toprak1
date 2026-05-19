# Toprak1 / IRONCLAD - Current State

## Current branch line

- Baseline branch: `baseline/local-import`
- Sprint 11 epic branch: `sprint/11-authority-evidence-hardening`
- Current epic goal: authority evidence hardening, dashboard artifact support, and repo state hygiene.

## Latest verified baseline

As of Sprint 11A:

- Authority status: `PASS_WITH_RISKS`
- Banned findings: `0`
- Suspicious findings: `229`
- Risk score: `943`
- Risk-bearing suspicious: `181`
- Static noise findings: `10`
- Telemetry risk findings: `23`
- Lua projection risk findings: `15`
- Scheduler state fallback review findings: `16`
- Test suite: `21 passed`

## What is usable now

The authority audit tool is usable as a local and CI-facing evidence generator.

Commands:

    python scripts/authority_audit.py --repo-root . --format dashboard --fail-on none
    python scripts/authority_audit.py --repo-root . --format dashboard --output docs/dashboard/artifacts/latest_authority.json --fail-on none
    python scripts/authority_audit.py --repo-root . --heatmap --fail-on none

The dashboard is usable as a local read-only command center. It must not expose write, approval, or authority mutation actions.

## What is not production-ready yet

Toprak1 runtime is not production-ready yet. Remaining blockers include:

- CI artifact-backed dashboard flow is only starting.
- Replay/evidence gate is not fully enforced.
- Governance local paths still need review.
- Lease/fencing paths still need review.
- Recovery/reconciliation paths still need review.
- Runtime staging, cold restart, archive, and replay semantics still need hardening.
- Redis/Docker operational setup is not yet stable across machines.
- Production observability and alerting are not complete.

## Current interpretation

`Banned: 0` means there are no merge-blocking authority findings right now.

`Suspicious: 229` does not mean 229 equal-risk issues. The current model separates static noise, telemetry, Lua projection, scheduler fallback, Lua authority transition, lease/fencing, governance, recovery, and true authority review buckets.

The current engineering target is to keep `Banned: 0` while reducing risk-bearing suspicious findings with evidence, not with blind waivers.

## Next planned Sprint 11 work

- 11B: add current state and readiness checklist.
- 11C: add dashboard artifact read-model verification.
- 11D: add replay evidence gate plan.
- 12A: document replay evidence gate plan.

## Sprint 12 status

Sprint 12 starts the replay evidence gate work. Current replay dashboard support is readiness-only; artifact-backed replay evidence is planned next.

## Sprint 12 status

Sprint 12 — Replay Evidence Gate MVP is complete.

Completed:

- 12A: Replay evidence gate plan documented.
- 12B: `scripts/replay_compare.py` can emit read-only JSON artifacts via `--output`.
- 12C: CI authority gate now generates and uploads `latest_replay.json`.
- 12D: Dashboard replay read model reads `latest_replay.json` when present, reports invalid artifacts read-only, and falls back to readiness when absent.

Latest verified Sprint 12 state:

- Test suite slice: `26 passed`
- Replay artifact status: `PASS`
- Replay artifact mode: `read-only`
- Replay artifact source: `replay_compare`
- Dashboard replay source behavior: `artifact` when present, `readiness` when absent
- Hard replay mismatch gate: planned for Sprint 13

## Sprint 14 Runtime Stabilization

Sprint 14A-14D completed on `fix/runtime-contracts`.

Completed:

- 14A: Runtime StateStore / WorkerConsumer compatibility inventory documented in `docs/architecture/runtime_contracts.md`.
- 14B: Runtime compatibility contracts restored:
  - `StateStore(redis)` legacy constructor compatibility.
  - Worker-facing lifecycle API compatibility.
  - `hfa_worker.executor.FakeExecutor` public import compatibility.
- 14C: Scheduler Lua fallback unified:
  - duplicate `_commit_fallback` removed.
  - extended fallback signature retained for priority, payload, trace, policy, region, control stream, and shard stream.
- 14D: Explicit StateStore compatibility contract test added.

Latest verified mini-gate:

- `30 passed`
- Coverage:
  - `tests/core/test_sprint14_state_store_compat_contract.py`
  - `tests/core/test_sprint11_idempotency.py`
  - `tests/core/test_sprint12_claim_renewal.py`
  - `tests/core/test_sprint13_run_api.py`
  - `tests/core/test_scheduler_lua_fallback_semantics.py`

Production readiness impact:

- Runtime compatibility drift reduced.
- Scheduler fallback override risk removed.
- Worker lifecycle compatibility is now explicitly tested.
- Deployment smoke repair remains next.
