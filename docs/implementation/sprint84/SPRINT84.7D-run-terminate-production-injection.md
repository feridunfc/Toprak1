# Sprint 84.7D — Canonical RUN_TERMINATE Production Injection

## Status

Implementation candidate against accepted parent `bcf1a1770426ca2525a0dc4804c8560f7256929c`.

This sprint is composition only. It does not redesign TASK terminal authority, RUN_TERMINATE authority, the run-finalization coordinator, or duplicate-delivery recovery.

## Selected canonical production profile

```text
WorkerService
  -> RunFinalizingWorkerConsumer
  -> RunFinalizingTaskConsumer
  -> canonical TASK_CLAIM
  -> executor
  -> _CanonicalTaskTerminalCompletionGateway
  -> TaskTerminalAuthorityBinding
  -> proof-bound TASK projection
  -> RunTerminationCoordinator
  -> RunTerminateAuthorityBinding
  -> immutable terminal TASK aggregate proof
  -> canonical RUN_TERMINATE
  -> proof-bound RUN projection
  -> ACK
```

The combined profile is selected only by the existing flags:

```text
canonical_task_terminal_binding=true
run_termination_binding_enabled=true
```

No new configuration or environment flag is added.

## Profile matrix

- A: both flags false — DagLua TASK completion, no RUN finalization change.
- B: canonical TASK terminal only — accepted 84.7C2 canonical TASK gateway.
- C: historical RUN finalization only — DagLua TASK completion wrapped by the historical coordinator path.
- D: both flags true — canonical TASK gateway wrapped by `RunTerminationCoordinator(authority_binding=RunTerminateAuthorityBinding(...))`.

Profile C remains intentionally compatible and is not silently canonicalized.

## Authority and ACK invariants

In Profile D, `DagLua.task_complete()` is not a reachable TASK terminal writer and the historical `run_terminate_from_tasks.lua` execution path is not a reachable RUN authority writer. Tests use exploding sentinels for both paths.

A non-last terminal TASK may produce RUN `NOT_READY`; this is successful TASK terminalization and permits ACK while RUN remains nonterminal. The last successful or failed TASK produces one canonical `RUN_TERMINATE` revision and projects the corresponding RUN result before ACK.

If canonical RUN authority commits but RUN projection fails, the message remains unacked. On redelivery, the existing `RunFinalizing*Consumer` terminal-duplicate path suppresses TASK claim/execution/terminal mutation and retries only RUN finalization. Exact canonical RUN replay reuses the durable receipt and completes projection without a second RUN revision.

If TASK terminal authority commits but TASK projection fails, RUN_TERMINATE is not attempted and the message remains unacked. Generic TASK crash/redelivery recovery remains Sprint 84.7E.

## Verification suites

D focused contracts freeze at 7 cases. D real-Redis production composition freezes at 6 cases, covering non-last NOT_READY, last success, last failure, RUN projection failure, RUN-only redelivery recovery, and the TASK-projection failure boundary.

Canonical RUN_TERMINATE authority regression is the accepted unit and real-Redis binding suites together: 23 cases.

Accepted regressions remain C2 22/3, C1 7/23/18, 84.7B 76, TASK_CLAIM 83, RUN finalization 41, production worker 43, Sprint 83.7 174, and Sprint 83.8 199, all with zero exclusions.

## Non-claims

```text
production_ready=false
production_cutover_authorized=false
TASK_REQUEUE canonical authority=false
```

84.7D does not authorize cutover and does not implement generic worker crash retry, TASK terminal repair, resource settlement, reconciliation, or controlled repair.
