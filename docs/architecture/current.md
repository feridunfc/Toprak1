
# Current Architecture

## Data flow
1. Admission accepts a run.
2. Scheduler consumes admitted events.
3. Runs are placed into per-tenant queues.
4. SchedulerLoop builds capacity snapshots.
5. WorkerScorer ranks candidates.
6. DispatchController gates pacing.
7. Lua/state transition layer commits atomically.
8. Worker plane executes.
9. Recovery, observability, and audit observe outcomes.

## Safety boundaries
- Auth is runtime-resolved and fail-closed in strict envs.
- State mutations go through atomic transition APIs.
- Backpressure hysteresis persists across cycles.
- Worker selection has one authoritative implementation.
