# Sprint 82.4 — Final Nine-Observation Closure

## Purpose

Sprint 82.4 is the acceptance closure for the eight callers and nine contradictory TASK/RUN scenarios first frozen by Sprint 80 diagnostics.

It does not rewrite or weaken the historical evidence. The immutable diagnostic remains:

```text
tests/diagnostics/sprint80/test_80_06_truth_contradictions.py
blob sha: ba3e1b8e89c64d3aa78de7aa5144a31e031850de
```

The Sprint 80 file describes the historical behavior. Sprint 82.4 adds a separate production acceptance suite that proves the post-Sprint-82 behavior.

## Global policy

Lifecycle truth remains operation-scoped:

- RUN query returns RUN-scoped truth and explicit TASK conflict metadata;
- TASK lifecycle writers use TASK truth and require compatible nonterminal RUN truth;
- RUN recovery uses RUN truth and validates the complete indexed TASK aggregate before mutation;
- missing authority, corruption and terminal/nonterminal contradiction fail closed;
- transport ACK for an explicitly identified terminal TASK duplicate is cleanup only and is not a lifecycle transition;
- the legacy RUN guard remains a compatibility reader and cannot authorize modern TASK mutation;
- repair remains an explicit reconciliation command;
- no caller silently chooses a winner and mutates the opposite truth plane.

## Exact nine observations

| # | Scenario | Production caller | Required result |
|---|---|---|---|
| 1 | RUN done, TASK running | control RUN query | return `done`; expose `run_terminal_task_nonterminal`; read-only |
| 2 | RUN running, TASK done | control RUN query | return `running`; expose `task_terminal_run_nonterminal`; read-only |
| 3 | RUN missing, TASK terminal | RUN recovery | durable `RUN_RECOVERY` candidate; zero lifecycle/projection mutation |
| 4 | RUN terminal, TASK running | RUN recovery | durable `RUN_RECOVERY` terminal conflict; preserve running projection |
| 5 | TASK missing, RUN terminal | TASK recovery | durable `TASK_REQUEUE` missing-task observation; zero requeue mutation |
| 6 | RUN terminal, TASK scheduled | modern WorkerConsumer claim path | no execution, no ACK, no TASK mutation; durable `TASK_CLAIM` conflict |
| 7 | RUN nonterminal, TASK terminal | modern terminal duplicate path | suppress claim/execution/completion; ACK only with exact task/run terminal evidence; query surfaces conflict |
| 8 | RUN terminal, TASK running | legacy RUN guard | suppress legacy execution; zero state mutation; no modern authority claim |
| 9 | RUN terminal, TASK ready | scheduler dispatch | no dispatch mutation/event; durable `TASK_DISPATCH` conflict |

## Acceptance invariants

The dedicated suite must contain exactly nine real-Redis tests and all must pass without skip or deselection.

For mutation callers, the suite verifies the relevant state, metadata, queues/ZSETs, output/event surfaces and conflict evidence. For read callers, the suite verifies scoped truth and explicit conflict metadata while preserving Redis state.

A failure is a product blocker. Tests must not be edited to accept historical behavior.

## Permitted implementation surface

Mandatory closure files:

```text
.github/workflows/sprint82-4-nine-observation-closure.yml
docs/implementation/sprint82/SPRINT82.4-nine-observation-closure.md
tests/integration/test_nine_observation_truth_closure.py
```

Runtime files may change only when the nine-observation suite proves a real gap:

```text
hfa-control/src/hfa_control/service.py
hfa-worker/src/hfa_worker/runtime/terminal_duplicate_delivery.py
hfa-worker/src/hfa_worker/consumer.py
```

No other production file is authorized without a separately documented blocker.

## Explicit exclusions

Sprint 82.4 does not:

- modify `tests/diagnostics/sprint80/*`;
- enable `HFA_CANONICAL_TASK_ADMIT_BINDING`;
- introduce automatic repair or reconciliation workers;
- terminalize RUN and TASK aggregates automatically;
- migrate the legacy worker to modern TASK ownership;
- change canonical authority records or feature flags;
- claim production readiness or authorize production cutover.

## Exit criteria

Sprint 82 closes only when:

1. the immutable Sprint 80 diagnostic blob is unchanged;
2. all nine current production observations pass on Redis 7;
3. Sprint 81.1 through Sprint 82.3 regressions remain green;
4. Sprint 82.2 and Sprint 82.3 protected workflows remain green;
5. Authority Gate reports no banned mutation;
6. compileall and `git diff --check` pass;
7. the PR remains unmerged until independent review and human authorization.
