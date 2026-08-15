# Sprint 85.0C — Historical Durable Effects + Cross-Aggregate Reconciliation

## Status

- Accepted parent: `be2ab4e620b2ab0c75a77943fe96d30b59165656`
- Scope: read-only reconciliation only
- Authority mutation: none
- Repair/replay: none
- New authority / OperationType / canonical index / receipt: none
- 85.1: locked

## Constitution

`OBSERVE -> DETECT -> CLASSIFY -> PROVE`

Canonical authority remains the existing aggregate authority store. Historical
stream/event/outbox data is not promoted to authority. All 85.0C reads start
from a known aggregate identity.

## Historical canonical enumeration

`RedisCanonicalAuthorityStore.load_aggregate_history()` is a read-only API over
three already-existing aggregate-local hashes:

- operation records
- operation receipts
- transition indexes

The reader sandwiches the full historical read between aggregate head A/B and
fails closed when the head changes. For head revision N it requires exactly N
valid records, receipts and indexes; revisions exactly 1..N; unique operation,
transition and revision identities; valid envelopes and canonical record hashes;
record/receipt/index continuity; state/revision continuity; and exact highest
record <-> aggregate head binding.

A missing required historical member is `BLOCKED_EVIDENCE /
CANONICAL_HISTORY_INCOMPLETE`. Malformed or contradictory canonical history is
`BLOCKED_EVIDENCE / CANONICAL_RECORD_CORRUPTION`. Canonical A/B instability is
`BLOCKED_EVIDENCE / CANONICAL_CHANGED_DURING_OBSERVATION`. Canonical history
corruption is never classified as DRIFT.

No global SCAN is used to discover aggregate truth and no stream is used as
canonical history.

## Reachability

All eight accepted 85.0C lifecycle operation families are discoverable from the
existing aggregate-local historical hashes after aggregate identity is known.
This is existing durable storage, not a new index.

The current exact-ID facade additionally derives RUN_CREATE and TASK_ADMIT from
current identity. TASK_DISPATCH, TASK_CLAIM, TASK_COMPLETE, TASK_FAIL and
TASK_REQUEUE require caller-supplied historical operation IDs. RUN_TERMINATE
also requires the exact operation ID unless an independently validated immutable
terminal proof is available; its identity requires `run_id +
terminal_proof_sha256`.

## RUN_TERMINATE <-> terminal proof

`TerminalAggregateProof` is durable runtime evidence. Its TASK rows are not
generic canonical TASK state.

85.0C therefore validates only:

`canonical RUN_TERMINATE record <-> immutable TerminalAggregateProof`

The binding covers every source-bound immutable proof field represented by the
canonical RUN_TERMINATE command/record: RUN/tenant identity, operation type and
operation ID, proof SHA, task/done/failed/skipped counts, exact tasks payload,
final state, finalized_at_ms, worker_instance_id, trigger_task_id,
trigger_terminal_state, and the structural revision/state continuity
`from_revision == canonical_expected_revision`,
`to_revision == canonical_expected_revision + 1`,
`previous_state == canonical_previous_state`, `next_state == final_state`.
Stable positive contradiction is `DRIFT / CANONICAL_PROOF_MISMATCH`. Missing or
corrupt proof is BLOCKED. The A/B proof fingerprint includes every immutable
TerminalAggregateProof field, including task rows, payload JSON, finalized/worker/
trigger fields and canonical expected revision/previous state. Any proof field
instability is `BLOCKED_EVIDENCE /
DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION`.

The implementation deliberately does not infer canonical TASK_COMPLETE,
TASK_FAIL, or any other canonical TASK terminal state from runtime `done`,
`failed`, `blocked_by_failure`, `dead_lettered`, `rejected`, `cancelled` or
`skipped`. It does not assert canonical all-TASK completeness for a RUN and does
not treat `run_tasks` as canonical membership authority.

## RUN_CREATE <-> resource reservation

The exact reservation immutable identity/proof is reconstructed from the durable
RUN_CREATE record and compared with a stable resource receipt. A required
immutable resource field that is absent is incomplete evidence and therefore
BLOCKED; a required field that is present but contradicts the canonical
reservation is `DRIFT / RESOURCE_PROOF_MISMATCH`. Malformed lifecycle metadata
or wrong Redis type is BLOCKED.

- `RESERVED` -> `BLOCKED_EVIDENCE / RESOURCE_FINALIZATION_PENDING`
- `FINALIZED` -> `CONSISTENT`
- `SETTLED` -> `CONSISTENT` for RUN_CREATE reservation identity/proof only
- `RELEASED` -> `DRIFT / RESOURCE_PROOF_MISMATCH`

The SETTLED terminal binding is checked separately.

## RUN_TERMINATE <-> SETTLED

Observation order is frozen as:

`RUN A -> terminal proof A -> settlement A -> settlement B -> terminal proof B -> RUN B`

An existing SETTLED receipt must exactly bind RUN_CREATE operation/proof,
RUN_TERMINATE operation, terminal proof SHA, canonical transition, record hash,
command hash, revision and final state. Stable contradiction is `DRIFT /
RESOURCE_PROOF_MISMATCH`.

If settlement is absent while durable per-RUN settlement applicability is not
proven, classification is `BLOCKED_EVIDENCE /
RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE`; absence alone never becomes DRIFT.

## Reason additions

Only these 85.0C reasons are added:

- `CANONICAL_HISTORY_INCOMPLETE`
- `RESOURCE_PROOF_MISMATCH`
- `RESOURCE_FINALIZATION_PENDING`
- `RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE`
- `DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION`

Existing `CANONICAL_RECORD_CORRUPTION`,
`CANONICAL_CHANGED_DURING_OBSERVATION` and `CANONICAL_PROOF_MISMATCH` are reused.

## Read-only safety

The `redis_persistence.py` change adds only immutable history result types,
read-only validation errors and `load_aggregate_history()`. Every pre-existing
function/method and the entire `RedisAuthorityKeyspace` class are CI-compared
against the accepted parent AST. No existing writer, key layout, Lua invocation,
commit behavior or digest format may change.

The new history method uses only existing snapshot/keyspace reads, Redis TYPE /
HGETALL and pure validation helpers. Reconciliation does not call commit,
mutation Lua, projection delivery, reserve/finalize/release/settle/capture or
repair/replay surfaces.

## R1.1 validation evidence contract

The dedicated 85.0C workflow freezes the corrected authored inventory at 49
unit test functions / 59 pytest cases and 21 real-Redis integration test
functions / 30 pytest cases. It also carries forward the accepted 85.0A 13-case
and 85.0B 64-case reconciliation regressions, the full accepted historical
regression matrix from 85.0B, and `scripts/authority_audit.py --fail-on banned`
with `banned == 0`. No skip/deselect/xfail/xpass is accepted.

Historical enumeration independently tests extra-member cardinality, a true
duplicate revision with cardinality held equal to head.revision, and a revision
hole that reaches the revision-chain validator.

## CI applicability

The accepted 85.0A and 85.0B workflow bodies remain byte-equivalent after
removing one new job-level applicability guard. Their original sprint PR
behavior remains unchanged while later reconciliation PRs no longer false-red
on historical feature-branch locks.

## Frozen deferrals

- canonical all-TASK completeness for RUN
- dispatch delivery completeness
- claim runtime-effect completeness
- TASK_COMPLETE fanout execution completeness
- TASK_FAIL fanout execution completeness
- per-TASK runtime-terminal -> canonical mapping
- missing settlement -> DRIFT without durable applicability proof
- new Redis index / receipt architecture
- global aggregate discovery
- repair / automatic repair
- authority/effect replay
- 85.1
