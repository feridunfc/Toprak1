# Sprint 83.3 — Durable user-facing status/result read model

## Boundary

```text
canonical RUN state/meta/result
→ typed deterministic read adapter
→ user-facing combined status/result view
```

This sprint introduces no lifecycle writer, persisted projection, consumer, scheduler change, worker change, reconciliation, retry, repair, or Loop Plane dependency.

```yaml
canonical_lifecycle_authority_changed: false
new_lifecycle_writer: false
query_path_writes: 0
Loop_Plane_dependency: NONE
runtime_mutation_effect: NONE
```

## Durable sources

The reader reuses the existing Redis records:

- `hfa:run:state:{run_id}`
- `hfa:run:meta:{run_id}`
- `hfa:run:result:{run_id}`

These records currently use 24-hour TTLs. The result stream remains supporting bounded evidence but is not required for every query. The view is durable only within the canonical retention window; it is not a historical archive.

## Verified external status vocabulary

Only values observed in current canonical RUN writers are mapped:

| Raw RUN state | Writer/operation | External status | Terminal |
| --- | --- | --- | --- |
| `admitted` | canonical RUN admission path | `QUEUED` | no |
| `running` | canonical RUN execution transition | `RUNNING` | no |
| `done` | `run_terminate_from_tasks.lua` / `RUN_TERMINATE` | `COMPLETED` | yes |
| `failed` | `run_terminate_from_tasks.lua` / `RUN_TERMINATE` | `FAILED` | yes |

Any other raw state is preserved in `internal_state` but classified as `UNKNOWN` with incomplete evidence. The adapter does not invent support for hypothetical RUN states.

## Read completeness vocabulary

- `RUNNING_WITHOUT_RESULT`
- `TERMINAL_WITH_RESULT`
- `TERMINAL_WITHOUT_RESULT`
- `UNKNOWN_RUN`
- `CONFLICTING_EVIDENCE`
- `EVIDENCE_INCOMPLETE`

A terminal state with a matching result record is `TERMINAL_WITH_RESULT`. A terminal state whose result hash is absent is `TERMINAL_WITHOUT_RESULT`. When metadata proves a result was expected, the reason is `RESULT_MISSING_OR_EXPIRED`; absence alone is not proof that Redis TTL expiry occurred.

`CONFLICTING_EVIDENCE` is reserved for contradictory durable facts, including a terminal result before canonical terminal state or a mismatch between RUN state and result status.

`EVIDENCE_INCOMPLETE` is used for malformed payloads, wrong Redis key types, empty state values, missing mandatory result identity and unsupported raw states.

## Freshness semantics

`freshness` describes the TTL of surviving evidence, not completeness of all expected records. Per-source TTL values are exposed as:

- `state_ttl_seconds`
- `meta_ttl_seconds`
- `result_ttl_seconds`

Therefore a terminal RUN may correctly report `freshness=CURRENT` together with `completeness=TERMINAL_WITHOUT_RESULT` when state and metadata survive but the result record is absent.

## Query surface

Existing contracts remain unchanged:

- `ControlPlaneService.get_run_state(run_id)`
- `ControlPlaneService.get_run_result(run_id)`

Sprint 83.3 adds:

- `ControlPlaneService.get_run_status_result(run_id)`

The combined method returns deterministic JSON-compatible data from `RunStatusResultView`. The reader invokes only Redis read operations (`TYPE`, `GET`, `HGETALL`, `TTL`) and never mutates lifecycle state.

The schema intentionally contains no projection revision, canonical revision or source transition field because the current durable RUN records do not provide a guaranteed contract for those values.

## Retention limitation

RUN state, metadata and result records currently expire after 24 hours. The reader does not extend TTLs. Missing records are classified rather than reconstructed or written back. Because no durable tombstone exists beyond current TTLs, a completely expired RUN is indistinguishable from a never-existing RUN and is reported as `UNKNOWN_RUN`.

## Explicit exclusions

- no new projection key or write trigger;
- no lifecycle authority change;
- no RUN/TASK mutation from query paths;
- no Loop Plane import;
- no production cutover;
- no Sprint 84 functionality.
