# Sprint 84.4A-R2 — Operation-Scoped Admission Resource Reservation

```yaml
sprint: 84.4A-R2
base: d76ac13027d036ddd699d3fb1d7f9ecb12ab38dc
purpose:
  unblock_canonical_RUN_CREATE: true
  boundary: precommit_admission_resource_reservation

protected_resources:
  - concurrent_run_quota
  - budget_reservation
  - tenant_inflight

operation_identity:
  format: "^run-create:v1:[0-9a-f]{64}$"
  proof: deterministic_length_prefixed_sha256

states:
  - RESERVED
  - FINALIZED
  - RELEASED

active_receipt_retention: persistent
active_counter_retention: persistent
released_receipt_retention_seconds: 604800
automatic_repair: false
automatic_ttl_normalization: false
legacy_QuotaManager_restored: false
AdmissionController_runtime_changed: false
canonical_RUN_CREATE_created: false
production_cutover: false
```

## Source and import boundary

The exact parent does not track `hfa.governance.quota_manager`. Sprint 84.4A-R2
does not restore or impersonate that legacy module. The dormant primitive is:

```text
hfa-core/src/hfa/governance/admission_resource_reservation.py
```

Future Sprint 84.4 code must import `AdmissionResourceReservationManager`
explicitly from that module. This sprint does not wire it into
`AdmissionController`; the exact-parent optional-import fallback remains
unchanged.

Tenant and system rate-limit tokens remain non-refundable request-attempt
accounting. They are outside the durable run-resource receipt.

## Redis boundary

The action-driven Lua script receives all keys explicitly:

```text
hfa:admission:resource-reservation:v1:<sha256(operation_id)>  HASH
hfa:quota:<tenant_id>:concurrent_runs                         STRING integer
hfa:quota:<tenant_id>:budget_reserved_cents                  STRING integer
hfa:tenant:<tenant_id>:inflight                              STRING integer
```

The inflight key is the existing key observed by `TenantRegistry.get_inflight()`.
The future canonical binding must use the manager's exact concurrent and budget
keys. No shadow resource counters are introduced.

## Atomic state machine

One Redis Lua execution implements each action:

```text
None      -> RESERVED
RESERVED  -> FINALIZED
RESERVED  -> RELEASED
FINALIZED -> FINALIZED exact duplicate
RELEASED  -> RELEASED exact duplicate
```

Changed immutable evidence under the same operation ID returns
`reservation_conflict` before resource inspection or mutation.

### First reservation

Before its first mutation, the script validates:

- the exact operation identity and proof;
- the Redis type of every existing protected counter;
- persistent TTL ownership for every existing protected counter;
- non-negative 53-bit-safe integer values;
- safe addition without exceeding `2^53 - 1`;
- concurrent, budget and inflight limits.

A missing counter is interpreted as zero. An existing counter is accepted only
when it is a persistent Redis STRING containing a valid non-negative safe
integer. Existing expiring counters fail closed with `resource_state_conflict`.
Their values and TTLs are not changed. Wrong Redis types also fail closed.

The primitive does not convert legacy TTL-bearing accounting into persistent
canonical accounting and does not repair malformed or missing ownership.

### Exact retry

An exact retry of a RESERVED or FINALIZED receipt validates persistent active
resource ownership. Missing, expiring, negative, non-integer, wrong-type or
undersized accounting returns `resource_state_conflict`. Exact retries do not
increment resources and do not normalize TTLs.

### Release

Release validates all three active counters before the first mutation:

```text
concurrent_runs >= 1
budget_reserved_cents >= exact receipt amount
tenant_inflight >= 1
```

Failure leaves the receipt RESERVED and performs zero mutation. Release does not
clamp counters and does not reconstruct accounting. Successful release decrements
all three counters exactly once, marks the receipt RELEASED and then applies the
seven-day receipt TTL.

### Finalize

Finalize changes only the receipt state and timestamp. It does not change any
protected counter and leaves the active receipt persistent.

## Lifetime and crash semantics

RESERVED and FINALIZED receipts are persistent. Active protected counters are
persistent. RELEASED receipts receive bounded seven-day idempotency retention.
The primitive never silently removes an existing counter TTL.

Python compensation is not authoritative because it cannot execute after process
termination. Future Sprint 84.4 must compose:

```text
reserve_once
-> canonical RUN_CREATE commit
-> finalize_once
-> replay-safe admitted projection
```

A pre-durability canonical failure calls `release_once`. A durable canonical
commit must not release resources merely because admitted projection is pending.

Terminal resource release or consumption ownership after canonical RUN creation
remains outside this sprint and must be closed before production cutover.

## Package resource contract

`hfa-core/pyproject.toml` explicitly packages:

```text
hfa/lua/admission_resource_reservation.lua
```

The packaging contract tests build a wheel, verify that the Lua member exists,
install the wheel into an isolated target and initialise the manager from that
installed package. Source-tree or editable-install availability alone is not
accepted as proof.

Sprint 84.4A-R2 implements no canonical RUN_CREATE, runtime admission binding,
automatic repair or production enablement.
