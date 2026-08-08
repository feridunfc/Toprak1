# Sprint 84.7A — Baseline Truth Closure

```yaml
sprint: 84.7A
base: 4f7048b8d0bc4ab429439a87121de4ad44fea71e
target_branch: sprint/84-7a-baseline-truth-closure
production_ready: false
production_cutover_authorized: false
```

## Goal

Close the two accepted baseline contradictions without entering Sprint 84.7B worker cutover scope:

1. remove the legacy RUN-state OCC/precheck from `SchedulerReservationDispatcher` while preserving TASK-scoped dispatch attempt/requeue metadata reads;
2. restore the explicit quarantine contract for `TaskClaimService.claim_legacy_direct_for_compatibility()`;
3. remove the Sprint 83.7/83.8 conditional scheduler test deselections;
4. finish with zero known baseline exclusions in the 84.7A gate.

## Scheduler authority boundary

`SchedulerReservationDispatcher` may read TASK metadata needed to construct canonical dispatch identity, specifically `requeue_count -> dispatch attempt`. It must not read `RedisKey.run_state(run_id)` to make a second RUN lifecycle authority/precheck decision.

The production composition therefore retains a Redis dependency only as TASK-attempt evidence. The previous `_check_run_state_is_dispatchable()` branch and `occ_state_conflict` result are removed. RUN transition legality belongs to the canonical dispatch/lifecycle authority path.

This is deliberately not implemented by setting the dispatcher Redis dependency to `None`, because doing so would silently force every dispatch attempt to `1` and destroy retry/requeue identity.

## TASK_CLAIM baseline failure

The exact baseline failing node is:

```text
tests/core/test_task_claim_service_legacy_quarantine_contract.py::test_legacy_direct_claim_has_explicit_compatibility_name
```

The test is correct. The compatibility method exists and is explicitly named, but its source-level quarantine documentation drifted from the locked contract. Sprint 84.7A restores the required warnings without changing claim routing, feature flags, worker composition, canonical authority behavior, or Lua.

## Explicit non-goals

- no WorkerService canonical TASK_CLAIM injection;
- no TASK_COMPLETE/TASK_FAIL canonicalization;
- no RUN_TERMINATE production injection;
- no TASK_REQUEUE/retry implementation;
- no resource settlement;
- no automatic repair/reconciliation;
- no production cutover.

## Acceptance

```text
scheduler known-baseline deselection = 0
TASK_CLAIM known baseline failure    = 0
Sprint 83.7 conditional deselection  = 0
Sprint 83.8 conditional deselection  = 0
mandatory skips/xfailed/xpassed      = 0
git diff --check                     = PASS
compileall                           = PASS
authority audit banned               = 0
```
