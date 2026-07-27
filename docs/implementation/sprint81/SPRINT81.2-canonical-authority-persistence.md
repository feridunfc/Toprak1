# Sprint 81.2 — Canonical Authority Redis/Lua Persistence Adapter

## Status

```yaml
implementation_slice: 81.2
status: TECHNICAL_SLICE_COMPLETE_READY_FOR_INDEPENDENT_REVIEW
base_branch: baseline/local-import
base_head: 34f7dd8e918829c55e6b64a5a09f623305dc9811

product_source_mutation: true
new_Redis_Lua_adapter: true
existing_runtime_writer_mutation: false
migration: false
runtime_cutover: false
production_ready_claim: false
```

## Scope

Sprint 81.2 implements the physical persistence boundary consumed by accepted
Sprint 81.1 `AuthorityCommitPlan` values. It does not route scheduler, worker,
control-plane or recovery traffic through the new store.

The adapter persists one accepted aggregate mutation as:

```yaml
authority_commit:
  aggregate_state_and_revision: 1
  canonical_operation_record: 1
  transition_uniqueness_index: 1
  immutable_operation_receipt: 1
  aggregate_transition_log_entry: 1
  aggregate_outbox_entry: 1

execution_boundary:
  Redis_Lua_script: canonical_authority_commit.lua
  one_script_execution: REQUIRED
  cluster_hash_slot: ONE_PER_AGGREGATE
  TTL: FORBIDDEN
```

## Physical key layout

All keys for one aggregate share `{canonical_aggregate_identity_sha256}` as the
Redis Cluster hash tag:

```text
hfa:authority:v1:{aggregate_sha256}:aggregate
hfa:authority:v1:{aggregate_sha256}:transition:{transition_id}
hfa:authority:v1:{aggregate_sha256}:receipt:{operation_id_sha256}
hfa:authority:v1:{aggregate_sha256}:operation-record:{operation_id_sha256}
hfa:authority:v1:{aggregate_sha256}:log
hfa:authority:v1:{aggregate_sha256}:outbox
hfa:authority:v1:{aggregate_sha256}:conflicts
```

The operation-record key is the immutable canonical record store entry used by
receipt-first proof resolution. Its key is stable when an incoming command
reuses the same operation ID with a different operation type, expected revision
or state. The transition key is an immutable transition-ID uniqueness index.

The aggregate hash is the current authority snapshot. The log and outbox are
append-only Redis streams. Conflict evidence is a separate stream and does not
consume aggregate revision.

## Lua evaluation order

```yaml
1: VALIDATE_COMMIT_PLAN_AND_REDIS_KEY_TYPES
2: RESOLVE_OPERATION_RECEIPT_AND_STABLE_OPERATION_RECORD
3: VALIDATE_STORED_RECEIPT_RECORD_AND_TRANSITION_INDEX
4: COMPARE_CANONICAL_COMMAND_AND_RECORD_HASHES
5: REJECT_INCOMPLETE_OR_COLLIDING_STORED_PROOF
6: VALIDATE_EXISTING_AGGREGATE_SNAPSHOT
7: COMPARE_EXPECTED_REVISION
8: COMPARE_PREVIOUS_STATE
9: WRITE_STATE_REVISION_INDEX_RECORD_RECEIPT_LOG_AND_OUTBOX
```

Receipt and operation-record lookup remain bound only to canonical aggregate
identity plus operation ID. Operation type, mutation class, expected revision
and state cannot bypass an existing receipt.

An existing aggregate hash is never treated as logical revision zero. Revision,
identity, state/null marker, last transition, last record hash and update time
must all form a complete authority snapshot before a subsequent revision may
commit.

## Outcomes

```yaml
receipt_present_same_command_and_record: ALREADY_APPLIED
receipt_present_different_command_or_record: IDEMPOTENCY_CONFLICT
record_or_receipt_without_its_pair: CANONICAL_RECORD_CORRUPTION_CONFLICT
transition_index_collision: CANONICAL_RECORD_CORRUPTION_CONFLICT
partial_or_invalid_aggregate_snapshot: CANONICAL_RECORD_CORRUPTION_CONFLICT
current_revision_less_than_expected: FUTURE_REVISION_CONFLICT
current_revision_greater_than_expected: STALE_REVISION_CONFLICT
create_revision_zero_existing_aggregate: AGGREGATE_ALREADY_EXISTS_CONFLICT
previous_state_mismatch: ILLEGAL_STATE_TRANSITION
accepted_commit: COMMITTED
```

All non-`COMMITTED` outcomes perform zero new lifecycle, transition-log or outbox
mutation.

## Stored proof rehydration

`RedisCanonicalAuthorityStore.load_receipt_probe()` loads the immutable receipt,
operation-indexed canonical record and transition uniqueness index. It validates
the record through the Sprint 81.1 contract, verifies all three stored objects
agree, and returns a `ReceiptProbe` suitable for receipt-first policy evaluation.

The codec is internal to the trusted `hfa.authority` package. It does not create
a new public writer authority or weaken the Sprint 81.1 threat model.

## Verification contract

The dedicated workflow uses isolated Redis 7 and requires:

```yaml
Sprint_81_1_policy_tests: 77_PASSED
Sprint_81_2_real_Redis_tests: 14_PASSED
focused_total: 91_PASSED
compileall: PASS
patch_whitespace: PASS
exact_changed_files: 6

required_behaviors:
  first_commit: COMMITTED
  exact_duplicate: ALREADY_APPLIED
  changed_command_same_operation_id: IDEMPOTENCY_CONFLICT
  changed_revision_and_operation_type_same_operation_id: IDEMPOTENCY_CONFLICT
  concurrent_same_revision_second_commit: STALE_REVISION_CONFLICT
  incomplete_stored_proof: CANONICAL_RECORD_CORRUPTION_CONFLICT
  missing_transition_index_blocks_rehydration: true
  partial_aggregate_is_not_revision_zero: true
  incomplete_committed_snapshot_blocks_next_revision: true
  wrong_Redis_key_type: FAIL_CLOSED_BEFORE_AUTHORITY_WRITE
  keys_have_no_TTL: true
  log_and_outbox_exactly_once: true
```

## Deliberate exclusions

```yaml
existing_task_admit_lua_modified: false
existing_task_dispatch_lua_modified: false
existing_task_claim_lua_modified: false
existing_task_complete_lua_modified: false
scheduler_or_worker_composition_modified: false
trusted_runtime_adapter_implemented: false
historical_revision_synthesis: false
legacy_key_migration: false
feature_flag_cutover: false
production_deployment: false
```

A later, separately reviewed Sprint 81.3 may connect one selected operation to
this persistence adapter under a feature flag. This document does not authorize
that connection.
