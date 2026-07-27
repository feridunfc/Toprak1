# Sprint 81.2 — Canonical Authority Redis/Lua Persistence Adapter

## Status

```yaml
implementation_slice: 81.2
status: CORRECTED_READY_FOR_INDEPENDENT_IMPLEMENTATION_RE_REVIEW
base_branch: baseline/local-import
base_head: 34f7dd8e918829c55e6b64a5a09f623305dc9811
head_binding: PR_EXACT_HEAD_AT_VERIFICATION_COMMENT

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

## Fixed aggregate-level key layout

Every key uses `{canonical_aggregate_identity_sha256}` as one Redis Cluster hash
tag:

```text
hfa:authority:v1:{aggregate_sha256}:aggregate
hfa:authority:v1:{aggregate_sha256}:transition-indexes
hfa:authority:v1:{aggregate_sha256}:receipts
hfa:authority:v1:{aggregate_sha256}:operation-records
hfa:authority:v1:{aggregate_sha256}:log
hfa:authority:v1:{aggregate_sha256}:outbox
hfa:authority:v1:{aggregate_sha256}:conflict-index
hfa:authority:v1:{aggregate_sha256}:conflicts
```

Transition indexes, receipts and operation records are fixed hashes. Their
fields are transition IDs or collision-safe operation-ID digests. This allows
Lua to resolve an existing operation proof and the current aggregate head
without deriving a key from the incoming transition ID.

Authority keys have no TTL.

## Accepted mutation atomicity

```yaml
authority_commit:
  aggregate_state_and_revision: 1
  canonical_transition_index: 1
  canonical_operation_record: 1
  immutable_operation_receipt: 1
  aggregate_transition_log_entry: 1
  aggregate_outbox_entry: 1

execution_boundary:
  Redis_Lua_script: canonical_authority_commit.lua
  one_script_execution: REQUIRED
  cluster_hash_slot: ONE_PER_AGGREGATE
  TTL: FORBIDDEN
```

## Stored proof integrity

Each immutable transition index, operation record and receipt is stored in a
Redis storage envelope:

```yaml
storage_envelope:
  payload: EXACT_JSON_BYTES
  storage_sha1: REDIS_LUA_COMPUTED
```

Before idempotency classification Lua:

1. loads the record and receipt through the stable operation field;
2. obtains the historical transition ID from the stored record;
3. loads that transition index independently of the incoming transition ID;
4. verifies every storage digest;
5. validates exact schemas and semantic receipt–record–index equality;
6. only then compares canonical command hashes.

For an exact duplicate, the stored record, receipt and index payload bytes must
also equal the incoming payload bytes.

```yaml
same_operation_same_command_and_exact_payloads: ALREADY_APPLIED
same_operation_different_command: IDEMPOTENCY_CONFLICT
stored_proof_missing_tampered_or_inconsistent: CANONICAL_RECORD_CORRUPTION_CONFLICT
```

The Redis storage digest is an additional corruption-detection layer. It does
not replace the accepted canonical SHA-256 record hash.

## Existing-head and history continuity

For every commit after revision one, the aggregate head is validated against:

```yaml
required_existing_head_proof:
  last_operation_id_and_digest: REQUIRED
  last_transition_index: PRESENT_AND_MATCHING
  last_operation_record: PRESENT_AND_STORAGE_VALID
  last_operation_receipt: PRESENT_AND_MATCHING
  transition_log_tail: MATCHING
  outbox_tail: MATCHING
```

The stream tails must match the aggregate revision, transition ID, record hash,
operation identity and stored payloads. Missing streams, missing proof objects,
or mismatched tails fail closed before any new lifecycle write.

## Durable conflict evidence

The Lua authority decision has access to a fixed conflict index and conflict
stream. These outcomes write conflict evidence in the same script execution:

```yaml
conflict_outcomes:
  - IDEMPOTENCY_CONFLICT
  - AGGREGATE_ALREADY_EXISTS_CONFLICT
  - CANONICAL_RECORD_CORRUPTION_CONFLICT

conflict_mutation:
  aggregate_revision_increment: 0
  canonical_record_count: 0
  operation_receipt_count: 0
  transition_log_mutation: 0
  outbox_mutation: 0
  durable_conflict_record_count: 1
```

Conflict identity is deterministically derived from aggregate identity,
operation ID, incoming command hash, stored command hash and conflict type.
`HSETNX` deduplicates repeated identical conflict observations; `XADD` occurs
only for the first insert.

The Python `record_conflict()` method remains only for explicit operator audit
notes. It is not used to complete a Lua conflict decision after the fact.

## Adapter read contracts

`load_receipt_probe()` verifies storage envelopes, canonical record semantics,
transition index equality and the accepted Sprint 81.1 stored-proof validator
against the requested aggregate and operation lookup before returning.

`get_aggregate_snapshot()` exact-validates:

```yaml
snapshot:
  exact_field_set: REQUIRED
  identity_sha256: REQUIRED
  revision: POSITIVE_JCS_SAFE_INTEGER
  state_is_null: EXACT_0_OR_1
  transition_id: NON_EMPTY
  canonical_record_hash: SHA256
  canonical_command_hash: SHA256
  operation_id: NON_EMPTY
  operation_digest: SHA256_AND_MATCHING_OPERATION_ID
  updated_at_ms: NON_NEGATIVE_JCS_SAFE_INTEGER
  projection_intents_json: JSON_ARRAY
```

## Lua evaluation order

```yaml
1: VALIDATE_COMMIT_PLAN_AND_CONFLICT_STORE
2: VALIDATE_REDIS_KEY_TYPES
3: RESOLVE_OPERATION_RECEIPT_AND_RECORD_BY_STABLE_OPERATION_FIELD
4: LOAD_HISTORICAL_TRANSITION_INDEX_FROM_STORED_RECORD
5: VALIDATE_STORAGE_DIGESTS_AND_STORED_PROOF_SEMANTICS
6: COMPARE_CANONICAL_COMMAND_HASH
7: VALIDATE_EXISTING_AGGREGATE_HEAD_AND_STREAM_CONTINUITY
8: COMPARE_EXPECTED_REVISION
9: COMPARE_PREVIOUS_STATE
10: WRITE_ACCEPTED_STATE_RECORD_RECEIPT_LOG_AND_OUTBOX
```

## Verification contract

The dedicated workflow uses isolated Redis 7 and requires:

```yaml
Sprint_81_1_policy_tests: PASS
expanded_Sprint_81_2_real_Redis_tests: PASS
compileall: PASS
patch_whitespace: PASS
exact_changed_files: 6

required_adversarial_coverage:
  changed_transition_with_missing_old_index: PASS
  stored_record_effect_tamper: PASS
  transition_index_extra_field: PASS
  previous_record_receipt_or_index_missing: PASS
  transition_log_or_outbox_missing: PASS
  aggregate_head_stream_tail_mismatch: PASS
  durable_conflict_atomicity_and_deduplication: PASS
  receipt_probe_lookup_binding: PASS
  snapshot_exact_validation: PASS
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
