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

## Sprint 16 Cold Restart Recovery

Sprint 16A-16D completed on `sprint/16-cold-restart-recovery`.

Completed:

- 16A: Cold restart + in-flight recovery contract documented in `docs/ops/cold_restart_recovery.md`.
- 16B: Read-only recovery audit script added:
  - `scripts/recovery_audit.py`
  - inspects `hfa:cp:running`, run meta, run state, and claim TTLs.
  - performs no Redis mutations.
- 16C: Recovery audit candidate detection tests added.
- 16D: Recovery audit artifact uploaded by Authority Gate CI.

Latest verified gates:

- `python scripts/recovery_audit.py --json` with `USE_FAKE_REDIS=1`
  - `status=PASS`
  - `running_count=0`
  - `candidate_count=0`
- `tests/core/test_recovery_audit.py`
  - `3 passed`

Production readiness impact:

- Cold restart recovery now has a read-only audit artifact.
- Stale/missing/expired in-flight run candidates are classified before mutation.
- Auto-recovery remains gated; no automatic requeue loop is enabled yet.

## Sprint 17 Proof-Gated Recovery Requeue

Sprint 17A-17D completed on `sprint/17-proof-gated-requeue`.

Completed:

- 17A: Proof-gated recovery requeue contract documented in `docs/ops/proof_gated_requeue.md`.
- 17B: Single-task proof-gated requeue command added:
  - `scripts/recovery_requeue.py`
  - reads recovery audit candidates.
  - fails closed when candidate is missing.
  - fails closed when replay/runtime/authority proof denies auto-resume.
  - supports dry-run artifact generation.
- 17C: Proof-gated requeue behavior tests added:
  - missing candidate blocks mutation.
  - dirty proof blocks mutation.
  - dry-run never mutates.
  - allowed candidate calls manager once.
  - proof denial reports fail-closed reasons.
- 17D: Recovery requeue dry-run artifact uploaded by Authority Gate CI.

Latest verified gates:

- `USE_FAKE_REDIS=1 python scripts/recovery_requeue.py --run-id ci-missing-run --tenant-id ci --dry-run --json`
  - `status=BLOCKED`
  - `requeue_status=NO_RECOVERY_CANDIDATE`
  - `mutation_attempted=false`
- `python -m pytest tests/core/test_recovery_requeue.py -q --tb=short`
  - `5 passed`
- Sprint 17 mini-gate:
  - recovery requeue + recovery audit + StateStore compatibility
  - `9 passed`

Production readiness impact:

- Recovery mutation now has an explicit proof-gated single-task command.
- Automatic recovery loop remains disabled.
- Requeue mutation remains routed through `TaskRecoveryManager.requeue_stale_task(...)`.

## Sprint 18 Artifact-Backed Recovery Proof

Sprint 18A-18D completed on `sprint/18-artifact-backed-recovery-proof`.

Completed:

- 18A: Artifact-backed recovery proof contract documented in `docs/ops/artifact_backed_recovery_proof.md`.
- 18B: `scripts/recovery_requeue.py` gained `--proof-mode artifacts`.
- 18C: Artifact-backed proof behavior tests added:
  - missing artifacts fail closed.
  - missing recovery candidate fails closed.
  - banned authority findings fail closed.
  - passing replay/authority/audit candidate allows proof.
- 18D: Authority Gate CI recovery requeue dry-run now uses artifact-backed proof mode.

Latest verified gates:

- `USE_FAKE_REDIS=1 python scripts/recovery_requeue.py --run-id ci-missing-run --tenant-id ci --proof-mode artifacts --dry-run --json`
  - `status=BLOCKED`
  - `proof_mode=artifacts`
  - `proof_allowed=false`
  - missing proof artifacts fail closed.
- Sprint 18 mini-gate:
  - `tests/core/test_artifact_backed_recovery_proof.py`
  - `tests/core/test_recovery_requeue.py`
  - `tests/core/test_recovery_audit.py`
  - `12 passed`

Production readiness impact:

- Recovery requeue proof is no longer limited to manual flags.
- Artifact-backed proof decisions are fail-closed.
- Automatic recovery loop remains disabled.
- Redis-backed mutation drill remains pending.

## Sprint 19 Redis-Backed Recovery Requeue Drill

Sprint 19A-19D completed on `sprint/19-redis-backed-recovery-requeue-drill`.

Completed:

- 19A: Redis-backed recovery requeue drill contract documented in `docs/ops/redis_backed_recovery_requeue_drill.md`.
- 19B: Controlled single-task Redis-backed drill script added:
  - `scripts/recovery_requeue_drill.py`
  - seeds a stale DAG task candidate.
  - writes clean replay/authority/recovery-audit proof artifacts.
  - invokes `recovery_requeue.py --proof-mode artifacts` without dry-run.
  - writes `docs/dashboard/artifacts/latest_recovery_requeue_drill.json`.
- 19C: Drill artifact tests added:
  - fake Redis is skipped as unsupported for Lua EVAL mutation drill.
  - PASS artifact shape is locked.
  - final recovery requeue artifact preserves artifact-backed proof metadata.
- 19D: Recovery requeue drill artifact uploaded by Authority Gate CI.

Latest verified gates:

- Real Redis local drill:
  - `python scripts/recovery_requeue_drill.py --json`
  - `status=PASS`
  - `requeue_status=TASK_REQUEUED`
  - `mutation_attempted=true`
  - task moved from `running` to `ready`
  - running zset removed
  - ready queue score written
  - `claim_epoch` remained monotonic/unchanged.
- Sprint 19 mini-gate:
  - `tests/core/test_recovery_requeue_drill.py`
  - `tests/core/test_artifact_backed_recovery_proof.py`
  - `tests/core/test_recovery_requeue.py`
  - `tests/core/test_recovery_audit.py`
  - `14 passed`

Production readiness impact:

- Artifact-backed proof can now authorize a real Redis-backed single-task requeue mutation.
- Canonical mutation path is `TaskRecoveryManager.requeue_stale_task(...)`.
- CI emits a skipped drill artifact under fake Redis because real Redis Lua EVAL is required.
- Automatic recovery daemon remains disabled.

## Sprint 20 Staging Cold Restart Drill

Sprint 20A-20D completed on `sprint/20-staging-cold-restart-drill`.

Completed:

- 20A: Staging cold restart drill contract documented in `docs/ops/staging_cold_restart_drill.md`.
- 20B: Controlled cold restart drill harness added:
  - `scripts/cold_restart_drill.py`
  - composes the Redis-backed recovery requeue drill.
  - uses artifact-backed proof.
  - invokes canonical recovery mutation path through the requeue drill.
  - writes `docs/dashboard/artifacts/latest_cold_restart_drill.json`.
- 20C: Cold restart drill artifact behavior tests added:
  - fake Redis is skipped as unsupported for Lua EVAL mutation drill.
  - PASS artifact shape is locked.
  - zombie completion rejection is explicitly marked pending.
- 20D: Cold restart drill artifact uploaded by Authority Gate CI.

Latest verified gates:

- Real Redis local cold restart drill:
  - `python scripts/cold_restart_drill.py --json`
  - `status=PASS`
  - `recovery_requeue_status=TASK_REQUEUED`
  - `mutation_attempted=true`
  - `proof_allowed=true`
  - task moved from `running` to `ready`
  - running zset entry removed
  - ready queue score written
  - `claim_epoch` remained monotonic/unchanged.
- Fake Redis CI-compatible artifact:
  - `status=SKIPPED`
  - `recovery_requeue_status=REAL_REDIS_REQUIRED`
- Sprint 20 mini-gate:
  - `tests/core/test_cold_restart_drill.py`
  - `tests/core/test_recovery_requeue_drill.py`
  - `tests/core/test_artifact_backed_recovery_proof.py`
  - `tests/core/test_recovery_requeue.py`
  - `tests/core/test_recovery_audit.py`
  - `16 passed`

Production readiness impact:

- Cold restart recovery path is now validated through a controlled drill harness.
- Artifact-backed proof and Redis-backed canonical requeue mutation are composed end-to-end.
- CI emits a skipped artifact under fake Redis because real Redis Lua EVAL is required.
- Zombie/stale-owner completion rejection remains pending explicit completion harness integration.
- Automatic recovery daemon remains disabled.

## Sprint 21 Zombie Completion Rejection Drill

Sprint 21A-21D completed on `sprint/21-zombie-completion-rejection-drill`.

Completed:

- 21A: Zombie completion rejection drill contract documented in `docs/ops/zombie_completion_rejection_drill.md`.
- 21B: Zombie completion rejection drill added:
  - `scripts/zombie_completion_drill.py`
  - composes Redis-backed recovery requeue drill.
  - attempts stale worker completion after requeue.
  - writes `docs/dashboard/artifacts/latest_zombie_completion_drill.json`.
- 21C: Zombie completion drill artifact behavior tests added:
  - fake Redis is skipped as unsupported for Lua EVAL mutation drill.
  - PASS artifact shape is locked.
- 21D: Zombie completion drill artifact uploaded by Authority Gate CI.

Latest verified gates:

- Real Redis zombie completion drill:
  - `python scripts/zombie_completion_drill.py --json`
  - `status=PASS`
  - `recovery_requeue_status=TASK_REQUEUED`
  - `zombie_completion_attempted=true`
  - `zombie_completion_accepted=false`
  - `zombie_completion_status=illegal_transition`
  - `pre_requeue_claim_epoch=1`
  - `post_requeue_claim_epoch=1`
- Fake Redis CI-compatible artifact:
  - `status=SKIPPED`
  - `zombie_completion_status=REAL_REDIS_REQUIRED`
- Sprint 21 mini-gate:
  - `tests/core/test_zombie_completion_drill.py`
  - `tests/core/test_cold_restart_drill.py`
  - `tests/core/test_recovery_requeue_drill.py`
  - `tests/core/test_artifact_backed_recovery_proof.py`
  - `tests/core/test_recovery_requeue.py`
  - `tests/core/test_recovery_audit.py`
  - `18 passed`

Production readiness impact:

- Stale/zombie worker completion is now explicitly rejected after recovery requeue.
- Requeue keeps `claim_epoch` monotonic/unchanged.
- Requeue clears stale worker identity fields.
- Post-requeue zombie completion fails closed via `illegal_transition`.
- Automatic recovery daemon remains disabled.

## Sprint 22B Cognitive Governance Hardening

Sprint 22B-A through 22B-C completed on `sprint/22b-cognitive-governance-hardening`.

Completed:

- 22B-A: Cognitive governance hardening contract documented in `docs/ops/cognitive_governance_hardening.md`.
- 22B-B: Cognitive governance boundary audit added:
  - `scripts/cognitive_governance_audit.py`
  - checks FeedbackWriter, SemanticBridge, cognitive executor, semantic memory, validation, and policy surfaces.
  - flags direct canonical runtime authority writes.
  - writes `docs/dashboard/artifacts/latest_cognitive_governance_audit.json`.
- 22B-C: Cognitive governance audit artifact uploaded by Authority Gate CI.

Latest verified gates:

- `python scripts/cognitive_governance_audit.py --json`
  - `status=PASS`
  - `checked_files=26`
  - `findings_count=0`
- `python -m pytest tests/core/test_cognitive_governance_audit.py -q --tb=short`
  - `2 passed`

Production readiness impact:

- Cognitive, semantic, and feedback surfaces are explicitly classified as advisory, feedback, memory, validation, policy, or governance-local.
- Hidden canonical runtime authority writes from cognitive surfaces are now statically audited.
- Runtime truth authority remains in runtime/Lua/control-plane canonical paths.
