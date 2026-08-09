# Sprint 84.7C2 — TASK Terminal Production Composition

## Status

Implementation candidate against accepted parent:

```text
a5cb395df414c773db56c0e57e8e07e43c7f3e73
```

Target branch:

```text
sprint/84-7c2-task-terminal-production-composition
```

This sprint is composition-only. It does not redesign the C1 terminal authority or `task_complete.lua` projector.

## Production composition

When `canonical_task_terminal_binding=false`, the accepted compatibility graph is preserved and `TaskConsumer` continues to receive the existing completion manager (`DagLua`, or the historical RUN-finalization coordinator when that separate profile is enabled).

When `canonical_task_terminal_binding=true`, WorkerService requires the full canonical dependency chain and composes:

```text
WorkerService
  -> WorkerConsumer
  -> TaskConsumer
  -> canonical TASK_CLAIM
  -> executor
  -> _CanonicalTaskTerminalCompletionGateway
  -> TaskTerminalAuthorityBinding.complete/fail
  -> durable canonical TASK terminal record + receipt
  -> C1 proof-bound projection
  -> WorkerConsumer ACK
```

The gateway is deliberately private and narrow. It only adapts the existing `TaskConsumer._complete_with_fence()` call shape to the accepted C1 binding:

- `terminal_state=done` -> `TaskTerminalAuthorityBinding.complete(...)`
- `terminal_state=failed` -> `TaskTerminalAuthorityBinding.fail(...)`
- any other terminal state -> fail closed

There is no fallback from canonical terminal authority failure to `DagLua.task_complete()`.

## Configuration

New WorkerService configuration:

```text
canonical_task_terminal_binding
```

Process-root environment:

```text
HFA_CANONICAL_TASK_TERMINAL_BINDING
```

Default is `false`. Values are parsed by the existing strict boolean parser.

When enabled, C2 requires:

```text
production = true
canonical_task_admit_binding = true
canonical_task_dispatch_binding = true
canonical_task_claim_binding = true
```

C2 also rejects:

```text
canonical_task_terminal_binding = true
run_termination_binding_enabled = true
```

Canonical RUN_TERMINATE production injection remains quarantined to Sprint 84.7D.

## Startup readiness

`TaskTerminalAuthorityBinding.initialise()` runs during `WorkerService.start()` before consumer-group preparation, shard lease acquisition, heartbeat startup, or consumer startup. If canonical terminal storage/projector initialization fails, worker startup fails closed and readiness is not advertised.

## ACK boundary

The accepted WorkerConsumer bridge already ACKs only after `TaskConsumer` returns a completion result with `completed=True`. The C2 gateway returns the C1 binding result directly, so the ACK boundary becomes:

```text
canonical terminal authority committed
AND
proof-bound terminal projection successful
THEN ACK
```

If authority commits but projection fails, the exception remains visible to the worker message boundary, the stream message remains unacked, the runtime task is not falsely terminalized, and no legacy terminal fallback runs.

## C2 acceptance tests

New focused contracts prove configuration, dependency-chain validation, composition shape, gateway routing, legacy default preservation, RUN-finalization incompatibility, and startup fail-closed ordering.

New real-Redis integration proves:

1. TaskRequested success -> canonical TASK_COMPLETE -> projection -> child success fanout -> XACK.
2. RunRequested failure -> canonical TASK_FAIL -> failure fanout -> stale ready membership removal -> XACK.
3. Canonical terminal authority commit followed by deterministic projection failure -> durable terminal head/receipt, runtime still nonterminal, message pending/unacked, RUN still nonterminal.

Both success and failure tests replace the selected WorkerService `DagLua.task_complete` method with an exploding sentinel. Passing therefore proves the old worker terminal writer is unreachable in the canonical-terminal profile.

## Frozen C1 product

C2 must preserve LF-normalized SHA256:

```text
hfa-control/src/hfa_control/task_terminal_authority.py
a5abd71bbb64410dc837fac625a3d0b0aa824d3fee0e3f68092066d1f2134778

hfa-core/src/hfa/lua/task_complete.lua
4bfcab69627f1b5fcc79331c13854f66f90cf8f437beff84c74b2f24f685ab48
```

`consumer.py`, `task_consumer.py`, C1 authority, RUN termination, `task_complete.lua`, and `task_claim_start.lua` are frozen for this sprint.

## Locked non-claims

```text
TASK_COMPLETE production binding candidate: true
TASK_FAIL production binding candidate: true
RUN_TERMINATE production injection: false
TASK_REQUEUE canonical authority: false
resource settlement: false
production_ready: false
production_cutover_authorized: false
```

Actual acceptance counts, Redis evidence, authority-audit status and CI status must be reported only from observed validator/CI output. This document does not convert an unexecuted gate into PASS evidence.
