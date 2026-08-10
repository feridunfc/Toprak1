# Sprint 84.8 — Canonical Resource Settlement

```yaml
accepted_parent: 66caff6349fb88c662bc5a3ea87b480bfe110ece
target_branch: sprint/84-8-canonical-resource-settlement
production_ready: false
production_cutover_authorized: false
TASK_REQUEUE_canonical_authority: false
```

## Authority boundary

84.8 adds no lifecycle operation and no RUN revision. Canonical `RUN_TERMINATE`
remains the terminal authority. Admission-resource settlement is a durable,
proof-bound effect that is allowed only after the exact `RUN_TERMINATE` record
and receipt are durable and validate as the current RUN head.

The selected order is:

```text
canonical TASK terminal
→ terminal TASK aggregate proof
→ canonical RUN_TERMINATE commit
→ exact RUN_CREATE record/receipt reconstruction
→ FINALIZED admission-resource receipt → SETTLED
→ RUN terminal projection
→ ACK
```

Historical Profile D/E remains unchanged while
`canonical_resource_settlement_binding=false`.

## Resource state machine

```text
None      → RESERVED
RESERVED  → FINALIZED
RESERVED  → RELEASED   # precommit compensation only
FINALIZED → SETTLED    # terminal settlement

FINALIZED → FINALIZED  exact duplicate
RELEASED  → RELEASED   exact duplicate
SETTLED   → SETTLED    exact settlement duplicate
```

`SETTLED` is persistent evidence. It decrements exactly once:

- `hfa:quota:{tenant_id}:concurrent_runs` by 1;
- `hfa:quota:{tenant_id}:budget_reserved_cents` by the exact RUN_CREATE estimate;
- `hfa:tenant:{tenant_id}:inflight` by 1.

Every counter must already be a persistent Redis STRING containing sufficient,
non-negative integer ownership. Missing/wrong-type/expiring/malformed or
insufficient accounting fails closed before the first mutation. There is no
clamp, reconstruction, TTL normalization, or automatic repair.

## Proof closure

Settlement binds the exact RUN_CREATE resource proof to the exact terminal
RUN authority evidence: RUN_TERMINATE operation ID, immutable terminal proof,
canonical transition/record/command hashes, revision and final state. Exact
replay returns `already_settled` and mutates no resource. Divergent proof under
a SETTLED receipt returns conflict and mutates nothing.

The RUN_CREATE helper only reconstructs `AdmissionResourceReservationInput`
from the already-strict canonical RUN_CREATE record validator. RUN_CREATE
command identity, ordering, revision, projection, and compensation rules are
unchanged.

## Worker composition

New opt-in configuration:

```text
canonical_resource_settlement_binding
HFA_CANONICAL_RESOURCE_SETTLEMENT_BINDING
```

Default is false. Enabling it requires production, canonical TASK terminal
binding, and RUN termination binding. The selected worker injects the existing
`AdmissionResourceReservationManager` into the existing
`RunTerminateAuthorityBinding`; no new worker adapter, coordinator, completion
gateway, authority store, aggregate, or operation type is introduced.

If the flag is enabled and the exact RUN_CREATE resource receipt is absent,
RUN projection and ACK are blocked.

## Crash/replay windows

- RUN_TERMINATE durable / settlement pending: no RUN projection, no ACK; exact
  redelivery reuses revision and settles once.
- SETTLED / RUN projection pending: no ACK; redelivery receives
  `already_settled`, performs zero second decrement, and replays projection.
- ACK loss after complete lifecycle: no executor, no TASK mutation, no RUN
  revision, no counter mutation; only exact durable replay and XACK.
- Non-last TASK: RUN remains NOT_READY; resource receipt remains FINALIZED and
  the current TASK delivery may ACK.

## Hard exclusions

84.8 does not touch scheduler/claim reservations, execution tokens, tenant-rate
attempt accounting, legacy `QuotaManager`, `decrement_tenant_inflight_if_needed`,
RecoveryService, TASK_REQUEUE, or `task_requeue.lua`. Historical resource
backfill/reconciliation/repair remains 85.0/85.1.

## Acceptance counts

Accepted-parent baseline evidence:

```text
admission resource unit source:        41
admission resource real Redis:         20
RUN_CREATE authority unit:             18
RUN_CREATE authority real Redis:       18
```

84.8 authored acceptance:

```text
focused: 66
  resource primitive unit:             49
  RUN_TERMINATE settlement contract:    9
  WorkerService composition:            8

real Redis: 20
  direct RUN settlement binding:        10
  WorkerService A-J end-to-end:         10
```

Locked historical regressions remain E 8/7, D 7/6/23, C2 22/3, C1 7/23/18,
84.7B 76, TASK_CLAIM 83, RUN finalization 41, production worker 43, Sprint 83.7
174, and Sprint 83.8 199, with zero hidden exclusions and authority banned=0.
