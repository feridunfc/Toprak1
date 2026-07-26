# Sprint 81.1 — Canonical Transition Policy-Evaluation Core

## Status

```yaml
implementation_slice: 81.1
status: CORRECTED_READY_FOR_FINAL_INDEPENDENT_RE_REVIEW
base_head: 75b7010f3dde2ee07aa123398eac903b5c6b0cd4
product_source_mutation: true
redis_lua_mutation: false
runtime_cutover: false
```

## Threat model and trust boundary

Sprint 81.1 selects **Model A — trusted-process boundary**.

```yaml
threat_model:
  arbitrary_in_process_python_caller: TRUSTED
  AuthorityEntryContext: TRUSTED_ADAPTER_INPUT
  internal_construction_token: ACCIDENTAL_MISUSE_GUARD_ONLY
  security_boundary: OUTSIDE_THIS_MODULE

later_trusted_authority_adapter:
  authenticate_principal: REQUIRED
  issue_operation_capability: REQUIRED
  bind_target_aggregate_identity: REQUIRED
  verify_lease_or_fence_against_authoritative_store: REQUIRED_WHEN_APPLICABLE
```

The module is therefore a **policy-evaluation core**, not a complete authentication or capability-issuance implementation. Public package APIs prevent unsupported accidental construction of proof objects, but they do not claim to defend against an arbitrary same-process Python caller.

## Receipt-first idempotency order

```yaml
evaluation_order:
  1: EVALUATE_TRUSTED_CONTEXT_CLAIMS
  2: RESOLVE_OPERATION_RECEIPT
  3: VALIDATE_STORED_RECEIPT_AND_RECORD_INDEPENDENTLY
  4: COMPARE_CANONICAL_COMMAND_HASH
  5: COMPARE_EXPECTED_REVISION
  6: VALIDATE_STATE_AND_PROJECTION_CONTRACT
```

Stored proof validation uses the stored receipt, stored canonical record, aggregate lookup identity and operation lookup ID. It does **not** compare stored revision, operation type or transition identity against mutable fields of the incoming command. After the stored proof is valid:

```yaml
same_operation_id_same_command_hash: ALREADY_APPLIED
same_operation_id_different_command_hash: IDEMPOTENCY_CONFLICT
```

Changing payload, expected revision, operation type or intended state under the same operation ID therefore produces `IDEMPOTENCY_CONFLICT`, not record corruption.

## Stale revision classification

```yaml
TASK_ADMIT_expected_revision_0_existing_aggregate: AGGREGATE_ALREADY_EXISTS_CONFLICT
RUN_CREATE_expected_revision_0_existing_aggregate: AGGREGATE_ALREADY_EXISTS_CONFLICT
all_other_stale_commands: STALE_REVISION_CONFLICT
```

Revision zero alone does not make a command a create operation.

## Projection receipt aggregate binding

`ProjectionApplicationReceipt` includes:

```yaml
canonical_aggregate_identity_sha256: REQUIRED
applied_revision: REQUIRED
applied_transition_id: REQUIRED
applied_record_hash: REQUIRED
```

A receipt bound to another aggregate returns `PROJECTION_CORRUPTION_CONFLICT` before revision ordering is evaluated.

## Preserved integrity guarantees

- collision-safe structured command identity;
- RFC 8785 JCS hashing with safe integer and exact-type rules;
- exact fifteen-operation registry;
- full canonical-record semantic validation;
- candidate validation before first store insert;
- actual-record duplicate proof;
- strict contiguous revision CAS;
- one accepted mutation → one revision, one canonical record and one operation receipt.

## Deliberate exclusions

This slice does not modify Redis/Lua scripts, choose a physical canonical-store layout, migrate existing keys, cut over runtime traffic, or claim production readiness.

## Final receipt-first operation-class correction

After the trusted authority-entry gate, operation receipt resolution precedes
operation mutation-class classification for every incoming operation type. The
receipt lookup key is canonical aggregate identity plus operation ID; operation
type and mutation class are command payload and cannot bypass an existing
receipt.

```yaml
receipt_present:
  same_operation_same_command: ALREADY_APPLIED
  same_operation_any_different_command: IDEMPOTENCY_CONFLICT
receipt_missing:
  unsupported_legacy_operation: UNSUPPORTED_LEGACY_OPERATION
  coordination_or_transport_operation: OPERATION_NOT_AUTHORITY_MUTATION
  accepted_authority_operation: CONTINUE_TO_REVISION_AND_STATE_CHECK
```

Regression coverage includes authority-to-coordination, authority-to-transport
and run-create-to-unsupported-legacy operation-ID reuse, plus the corresponding
receipt-missing classifications.
