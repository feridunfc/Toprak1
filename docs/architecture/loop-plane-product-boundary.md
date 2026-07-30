# Loop Plane product boundary

Loop Plane is an execution-outcome control plane. Runtime lifecycle authority remains in canonical TASK/RUN authority. Runtime reconciliation remains a separate plane.

Modes are enforced:

- `OFF`: no observation, event, receipt, decision, or proposal write.
- `OBSERVE`: translated runtime observations only.
- `SHADOW_DECIDE`: observation and evaluation; no proposal.
- `PROPOSE`: passive, non-executable, human-gated proposal creation; no command submission.

The current store in this lane is explicitly `InMemoryPrototypeLoopStore`. Durable distributed persistence, status projection checkpoints, automatic command submission and production cutover are not implemented in this lane.

Runtime mutation APIs, scheduler mutation, worker mutation, Redis RUN/TASK lifecycle writes and automatic repair/retry/rework/replan remain forbidden.
