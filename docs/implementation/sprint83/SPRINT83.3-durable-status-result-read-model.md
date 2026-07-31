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

These records currently use 24-hour TTLs. The result stream remains supporting bounded evidence but is not required for every query.

## External status vocabulary

| Internal state | External status |
| --- | --- |
| admitted, queued, pending, scheduled, rescheduled | `QUEUED` |
| running | `RUNNING` |
| done | `COMPLETED` |
| failed, rejected, dead_lettered | `FAILED` |
| cancelled | `CANCELLED` |
| missing or unsupported | `UNKNOWN` |

## Completeness vocabulary

- `RUNNING_WITHOUT_RESULT`
- `TERMINAL_WITH_RESULT`
- `TERMINAL_WITHOUT_RESULT`
- `UNKNOWN_RUN`
- `INCOMPLETE_PROJECTION`
- `CONFLICTING_EVIDENCE`
- `RESULT_EXPIRED`
- `EVIDENCE_INCOMPLETE`

A terminal state with a matching result record is `TERMINAL_WITH_RESULT`. A terminal state whose metadata binds a `result_event_id` but whose result hash is absent is `RESULT_EXPIRED`. A result before canonical terminal state, mismatched state/result status, malformed payload, wrong key type, or missing terminal result event identity is fail-closed as `CONFLICTING_EVIDENCE`.

## Query surface

Existing contracts remain unchanged:

- `ControlPlaneService.get_run_state(run_id)`
- `ControlPlaneService.get_run_result(run_id)`

Sprint 83.3 adds:

- `ControlPlaneService.get_run_status_result(run_id)`

The combined method returns deterministic JSON-compatible data from `RunStatusResultView`. The reader invokes only Redis read operations (`TYPE`, `GET`, `HGETALL`, `TTL`) and never mutates lifecycle state.

## Retention limitation

RUN state, metadata and result records currently expire after 24 hours. The reader does not extend TTLs. Missing records are classified rather than reconstructed or written back. Because no durable tombstone exists beyond current TTLs, a completely expired RUN is indistinguishable from a never-existing RUN and is reported as `UNKNOWN_RUN`.

## Explicit exclusions

- no new projection key or write trigger;
- no lifecycle authority change;
- no RUN/TASK mutation from query paths;
- no Loop Plane import;
- no production cutover;
- no Sprint 84 functionality.
