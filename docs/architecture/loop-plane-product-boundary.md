# Loop Plane shadow-domain boundary

Loop Plane is an execution-outcome control plane. Runtime lifecycle authority remains in canonical TASK/RUN authority. Runtime reconciliation remains a separate plane.

## Enforced modes

- `OFF`: no observation, event, receipt, decision, or proposal write.
- `OBSERVE`: translated runtime observations only.
- `SHADOW_DECIDE`: observation and explicit semantic evaluation; no proposal.
- `PROPOSE`: passive, non-executable, human-gated proposal creation; no command submission.

## Creation and observation ordering

```text
TaskAdmitted + immutable LoopContract
→ LoopStarted

TaskClaimed + claim fence + execution generation + input hash
→ AttemptObserved

TaskCompleted / TaskFailed / RunCompleted / RunFailed
→ RuntimeObservationRecorded

explicit evaluator command
→ CriteriaEvaluated
→ ShadowDecisionRecorded

explicit proposal command
→ ProposalRecorded
```

Runtime terminal facts never become semantic criteria automatically.

## Decision lifecycle

- `ACCEPT` closes the loop.
- `ESCALATE` closes the loop for human handling.
- `BLOCK` and `UNKNOWN` remain nonterminal recommendations.
- `RETRY_RECOMMENDED`, `REWORK_RECOMMENDED`, and `REPLAN_RECOMMENDED` remain non-executable recommendations.
- One decision is allowed per observed attempt. A later runtime claim creates a new attempt boundary.
- `max_attempts` and `max_rework_depth` are enforced by the reducer.

## Runtime event compatibility matrix

| Actual/runtime-facing type | Inspected source | Loop translation | Supported now | Exact limitation |
|---|---|---|---|---|
| `TaskAdmitted` | canonical task admission path; expected Lua source `hfa-core/src/hfa/lua/task_admit.lua` | `LoopStarted` | Conditional | Requires an externally supplied immutable `LoopContract`; contract identity is not invented from runtime state. |
| `TaskClaimed` | worker/task claim lifecycle path | `AttemptObserved` | Conditional | Requires canonical operation ID, source transition ID, task revision, claim fence, execution generation, and input hash. |
| `TaskCompleted` | TaskConsumer completion lifecycle path | `RuntimeObservationRecorded` | Conditional | Runtime fact only; semantic evidence must be supplied by an explicit evaluator. |
| `TaskFailed` | task failure lifecycle path | `RuntimeObservationRecorded` | Conditional | Runtime fact only; missing provenance is classified. |
| `RunCompleted` | `hfa-core/src/hfa/lua/run_terminate_from_tasks.lua` results stream | `RuntimeObservationRecorded` | Conditional | Sprint 83.2 result stream does not by itself provide the complete Loop provenance contract. |
| `RunFailed` | `hfa-core/src/hfa/lua/run_terminate_from_tasks.lua` results stream | `RuntimeObservationRecorded` | Conditional | Same provenance limitation as `RunCompleted`. |

Unknown event types fail closed. Missing contract identity or claim provenance returns `PROVENANCE_INSUFFICIENT`; no values are inferred from mutable Redis state.

## Prototype-store limitations

The current store is explicitly `InMemoryPrototypeLoopStore`.

```yaml
status_projection: NOT_IMPLEMENTED
checkpoint_persistence: NOT_IMPLEMENTED
durable_store: NOT_IMPLEMENTED
distributed_atomicity: NOT_IMPLEMENTED
automatic_command_submission: false
production_cutover: false
```

Runtime mutation APIs, scheduler mutation, worker mutation, Redis RUN/TASK lifecycle writes and automatic repair/retry/rework/replan remain forbidden.
