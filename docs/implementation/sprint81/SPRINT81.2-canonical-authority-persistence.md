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

Sprint 81.2 preserves the accepted Sprint 81.1 canonical-store classifier.
`ALREADY_APPLIED` requires the same deterministic transition ID, canonical
record hash and exact immutable record, receipt and transition-index payloads.
Canonical command hash equality alone is not sufficient.

```yaml
same_operation_same_exact_canonical_record: ALREADY_APPLIED
same_operation_same_command_but_different_record: CANONICAL_RECORD_CORRUPTION_CONFLICT
same_operation_different_command: IDEMPOTENCY_CONFLICT
stored_proof_missing_tampered_or_inconsistent: CANONICAL_RECORD_CORRUPTION_CONFLICT
```

Concurrent evaluators for one operation must therefore receive stable
operation-level commit metadata (`committed_at_ms`, authority writer identity
and correlation identity where present). Creating that stable metadata is a
trusted-adapter precondition and remains outside this persistence-only slice.

The Redis storage digest is an additional corruption-detection layer. It does
not replace the accepted canonical SHA-256 record hash. Before each Lua commit,
the trusted Python adapter reconstructs stored records with the Sprint 81.1
canonical validator, including canonical hash recomputation, deterministic
transition identity, structured aggregate identity, operation contract and
projection-intent validation. Lua compares SHA-1 digests of the exact Redis
storage envelopes observed by that prevalidation; a race causes an internal
retry rather than an unvalidated authority decision.

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
operation identity and stored payloads. The transition-index, receipt and record
hash cardinalities and both stream lengths must also equal the current revision.
Deletion or insertion anywhere in those persisted collections therefore fails
closed before any new lifecycle write.

```yaml
current_head_continuity: PROVEN
historical_cardinality_continuity: PROVEN
full_historical_content_rehash: NOT_YET_PROVEN
historical_content_tamper_detection: DEFERRED_TO_SPRINT_85
```

Sprint 81.2 does not claim that every non-head historical payload is rehashed on
every commit. Full replay reconstruction and historical content verification
remain a separately reviewed Sprint 85 responsibility.

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
The authority-conflict index contains a reserved monotonic evidence-count field.
Before every decision, index cardinality and stream length must equal that count.
`HSETNX` deduplicates repeated identical conflict observations; `XADD` occurs
only for the first insert.

```yaml
authority_conflict_pair:
  both_absent_before_first_conflict: ALLOWED
  both_present_with_matching_count: ALLOWED
  one_missing_after_prior_conflict: CONFLICT_EVIDENCE_STORE_UNAVAILABLE
  deleted_index_entry_or_stream_row: CONFLICT_EVIDENCE_STORE_UNAVAILABLE
```

If either conflict store has the wrong Redis type or the authority evidence pair
is incomplete, the script cannot truthfully claim durable conflict evidence. It
returns `CONFLICT_EVIDENCE_STORE_UNAVAILABLE`, performs zero lifecycle mutation
and requires operator/reconciliation handling.

The Python `record_conflict()` method writes to a separate operator-audit stream.
Operator notes cannot alter authority conflict pair cardinality and are never
used to complete a Lua conflict decision after the fact.

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
1: PYTHON_CANONICAL_PREVALIDATE_OPERATION_AND_HEAD_PROOFS
2: VALIDATE_COMMIT_PLAN_AND_AUTHORITY_CONFLICT_PAIR
3: VALIDATE_REDIS_KEY_TYPES
4: BIND_LUA_READS_TO_PREVALIDATED_RAW_ENVELOPE_DIGESTS
5: RESOLVE_OPERATION_RECEIPT_AND_RECORD_BY_STABLE_OPERATION_FIELD
6: LOAD_HISTORICAL_TRANSITION_INDEX_FROM_STORED_RECORD
7: VALIDATE_STORAGE_DIGESTS_AND_STORED_PROOF_SEMANTICS
8: APPLY_EXACT_CANONICAL_STORE_CLASSIFIER_PARITY
9: VALIDATE_EXISTING_AGGREGATE_HEAD_AND_STREAM_CONTINUITY
10: COMPARE_EXPECTED_REVISION_AND_PREVIOUS_STATE
11: WRITE_ACCEPTED_STATE_RECORD_RECEIPT_LOG_AND_OUTBOX
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
  Redis_and_core_classifier_same_outcome: PASS
  refreshed_storage_digest_canonical_tamper: PASS
  authority_conflict_pair_loss_detection: PASS
  conflict_store_unavailable_result: PASS
  old_revision_proof_deletion: PASS
  old_stream_entry_deletion: PASS
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
operation_digest_trust_boundary:
  issued_and_recomputed_by_Python_adapter: true
  direct_Lua_invocation: FORBIDDEN
  runtime_wiring_before_independent_binding_review: FORBIDDEN
historical_revision_synthesis: false
full_historical_content_rehash: false
legacy_key_migration: false
feature_flag_cutover: false
production_deployment: false
```

A later, separately reviewed Sprint 81.3 may connect one selected operation to
this persistence adapter under a feature flag. This document does not authorize
that connection.
