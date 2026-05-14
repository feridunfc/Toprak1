# Authority Matrix

| Area | Authoritative owner | Non-authoritative/projection participants | Sprint 1 rule |
| --- | --- | --- | --- |
| Durable lifecycle truth | Event log | Redis state keys, task output records, metrics | Authoritative writes should append events first where gated. |
| Runtime lifecycle projection | Control-plane state store | Redis keys under `hfa:dag:*` | Projection may be updated only after gated event append when `IRON_V3_EVENT_GATE` is enabled. |
| Scheduling decision | Control plane scheduler | Workers, semantic sidecars, metrics | Scheduler remains singular; Sprint 1 does not modify scheduler modules. |
| Worker execution | Worker runtime | Control plane completion handler | Worker output is an effect report, not durable truth by itself. |
| Budget enforcement | `BudgetGuard` Redis namespace | Callers, logs, display USD helpers | `BudgetGuard.authority_source` identifies the single source. Strict mode forces fail-closed. |
| Semantic advice | Semantic advisory hooks | Scheduler/agents consuming hints | Advisory failures may degrade with observable verdicts. |
| Semantic gate | Explicit gate-mode semantic hook | Advisory sidecar state | Gate failures deny/fail closed with observable verdicts. |
| Correction feedback | Single normalized result handler | Agent-specific result producers | `handle_result` emits one correction-loop decision per result. |

## Boundary notes

Redis remains necessary for leases, queues, counters, projections, and recovery
helpers.  The constitutional boundary is not "no Redis writes"; it is "no
unexplained authoritative truth without a durable event in gated paths."
