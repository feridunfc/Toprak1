# Sprint 81.2 — Canonical Authority Redis/Lua Persistence Adapter

## Status

```yaml
implementation_slice: 81.2
status: STARTED_TECHNICAL_IMPLEMENTATION
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
  canonical_transition_record: 1
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
hfa:authority:v1:{aggregate_sha256}:log
hfa:authority:v1:{aggregate_sha256}:outbox
hfa:authority:v1:{aggregate_sha256}:conflicts
```

The aggregate hash is the current authority snapshot. Transition records and
receipts are immutable string values. The log and outbox are append-only Redis
streams. Conflict evidence is a separate stream and does not consume aggregate
revision.

## Lua evaluation order

```yaml
1: VALIDATE_COMMIT_PLAN_AND_REDIS_KEY_TYPES
2: RESOLVE_OPERATION_RECEIPT
3: VALIDATE_STORED_RECEIPT_AND_RECORD
4: COMPARE_CANONICAL_COMMAND_AND_RECORD_HASHES
5: REJECT_RECORD_WITHOUT_RECEIPT
6: COMPARE_EXPECTED_REVISION
7: COMPARE_PREVIOUS_STATE
8: WRITE_STATE_REVISION_RECORD_RECEIPT_LOG_AND_OUTBOX
```

Receipt lookup remains bound only to canonical aggregate identity plus operation
ID. Operation type, mutation class, expected revision and state cannot bypass an
existing receipt.

## Outcomes

```yaml
receipt_present_same_command_and_record: ALREADY_APPLIED
receipt_present_different_command_or_record: IDEMPOTENCY_CONFLICT
record_without_receipt: CANONICAL_RECORD_CORRUPTION_CONFLICT
current_revision_less_than_expected: FUTURE_REVISION_CONFLICT
current_revision_greater_than_expected: STALE_REVISION_CONFLICT
create_revision_zero_existing_aggregate: AGGREGATE_ALREADY_EXISTS_CONFLICT
previous_state_mismatch: ILLEGAL_STATE_TRANSITION
accepted_commit: COMMITTED
```

All non-`COMMITTED` outcomes perform zero new lifecycle, transition-log or outbox
mutation.

## Stored proof rehydration

`RedisCanonicalAuthorityStore.load_receipt_probe()` loads the immutable receipt
and its canonical transition record, validates both through the Sprint 81.1
contract, and returns a `ReceiptProbe` suitable for receipt-first policy
evaluation.

The codec is internal to the trusted `hfa.authority` package. It does not create
a new public writer authority or weaken the Sprint 81.1 threat model.

## Verification contract

The dedicated workflow uses isolated Redis 7 and requires:

```yaml
Sprint_81_1_policy_tests: PASS
Sprint_81_2_real_Redis_tests: PASS
compileall: PASS
patch_whitespace: PASS
exact_changed_files: 6

required_behaviors:
  first_commit: COMMITTED
  exact_duplicate: ALREADY_APPLIED
  changed_command_same_operation_id: IDEMPOTENCY_CONFLICT
  concurrent_same_revision_second_commit: STALE_REVISION_CONFLICT
  record_without_receipt: CANONICAL_RECORD_CORRUPTION_CONFLICT
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
