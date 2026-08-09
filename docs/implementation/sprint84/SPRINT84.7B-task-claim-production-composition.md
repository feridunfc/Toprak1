# Sprint 84.7B — Canonical TASK_CLAIM Production Composition

## Status

Implementation candidate against accepted parent:

```text
f41f5a2f0ab1c6866f4132574027f3e0519296e7
```

Target branch:

```text
sprint/84-7b-task-claim-production-composition
```

## Purpose

Sprint 84.7B does not redesign TASK_CLAIM. The canonical TASK_CLAIM command,
authority binding, durable receipt semantics, projection and retry behavior were
already proven before this sprint.

The remaining gap was production composition: `WorkerService` constructed
`TaskClaimManager(self._dag_lua)` without passing the canonical TASK_ADMIT,
TASK_DISPATCH and TASK_CLAIM binding configuration. Therefore the real worker
could still execute `TaskClaimManager.claim_start()` through its legacy path even
though the canonical manager existed.

This sprint binds the existing manager into the real worker composition and
closes the production stream-routing boundary so a canonical-claim-enabled
`WorkerConsumer` cannot fall through to `IdempotencyGuard.try_claim_and_mark_running()`.

## Locked architecture

```text
process environment
    │
    ├── HFA_CANONICAL_TASK_ADMIT_BINDING
    ├── HFA_CANONICAL_TASK_DISPATCH_BINDING
    └── HFA_CANONICAL_TASK_CLAIM_BINDING
    │
    ▼
strict Worker process-root configuration
    │
    ▼
WorkerService dependency validation
    │
    ▼
TaskClaimManager
    │
    ├── disabled → existing legacy claim_start path
    │
    └── enabled
          │
          ▼
      WorkerConsumer explicit canonical-claim routing dependency
          │
          ├── TaskRequested ───────┐
          │                        ├── TaskConsumer.consume_once()
          └── RunRequested ────────┘
                                   │
                                   ▼
                           TaskClaimManager.claim_start()
                                   │
                                   ▼
                           TaskClaimAuthorityBinding
                                   │
                                   ▼
                           RedisCanonicalAuthorityStore
                                   │
                                   ▼
                           canonical claim projection
                                   │
                                   ▼
                           execution authorization
```

Global safe defaults remain `false`. Sprint 84.7B does not silently flip a
production feature flag.

When canonical TASK_CLAIM is enabled, both canonical TASK_ADMIT and canonical
TASK_DISPATCH dependency assertions are required. Missing dependencies are a
configuration error before task execution.

Actual claim safety does not rely only on those booleans: the existing authority
binding still validates durable canonical dispatch evidence, reservation owner,
worker identity and scheduler epoch before committing TASK_CLAIM.

## Invariants

```text
canonical claim enabled
→ production worker required
→ TASK_ADMIT dependency asserted
→ TASK_DISPATCH dependency asserted
→ TaskClaimManager canonical path selected
→ canonical dispatch evidence required
→ canonical authority commit before claim projection
→ only successful first projection allows execution
```

The compatibility-only `claim_legacy_direct_for_compatibility()` surface remains
quarantined and is not introduced into the worker runtime graph. When canonical
TASK_CLAIM routing is enabled, the older `IdempotencyGuard.try_claim_and_mark_running()`
path is unreachable for supported worker request envelopes.

## Safe defaults

```text
HFA_CANONICAL_TASK_ADMIT_BINDING     default false
HFA_CANONICAL_TASK_DISPATCH_BINDING  default false
HFA_CANONICAL_TASK_CLAIM_BINDING     default false
```

Therefore the new routing override is active only when the explicit canonical
claim dependency is enabled. `RUNTIME_INTERNAL` and historical test/dev configurations therefore retain their
existing behavior unless the canonical claim chain is explicitly enabled.

## Exact scope

```text
.github/workflows/sprint84-7b-task-claim-production-composition.yml
.github/workflows/sprint83-4-production-run-finalization-binding.yml
docs/implementation/sprint84/SPRINT84.7B-task-claim-production-composition.md
hfa-worker/src/hfa_worker/consumer.py
hfa-worker/src/hfa_worker/main.py
hfa-worker/src/hfa_worker/process_root.py
tests/core/test_production_worker_composition_contract.py
tests/core/test_production_worker_process_root_contract.py
tests/integration/test_worker_task_claim_canonical_composition_integration.py
```

The Sprint 83.4 workflow entry is regression-gate maintenance only: the
production regression suite expanded from 31 to 43 tests and all 43 pass. No
Sprint 83.4 runtime behavior is changed by this maintenance update.

## Explicit non-claims

Sprint 84.7B does not implement or claim:

```text
TASK_COMPLETE canonical runtime binding
TASK_FAIL canonical runtime binding
RUN_TERMINATE worker production injection
TASK_REQUEUE / retry cutover
resource settlement
automatic recovery
automatic repair
Loop Plane lifecycle mutation
production_ready
production_cutover_authorized
```

Those remain later master-plan steps.

## Acceptance

Mandatory acceptance includes:

- exact-parent / exact-branch scope gate;
- strict environment parsing and dependency failure tests;
- existing TASK_CLAIM authority regression with zero skips/deselects;
- real Redis stream-route test proving `TaskRequested -> WorkerConsumer._process_message()
  -> TaskConsumer.consume_once() -> canonical TASK_CLAIM`;
- negative real Redis test proving canonical-claim-enabled `RunRequested` cannot
  execute `IdempotencyGuard.try_claim_and_mark_running()`;
- controlled executor sentinel immediately after claim so TASK_COMPLETE/TASK_FAIL
  remain outside Sprint 84.7B;
- Sprint 83.7 regression parity: 174 passed, zero deselections;
- Sprint 83.8 regression parity: 199 passed, zero deselections;
- authority audit with `banned=0`;
- `compileall`;
- `git diff --check`.

## Result rule

The sprint may be marked PASS only after the mandatory real-Redis validator has
run with zero skips/deselects/xfails and the exact changed-file scope is clean.
