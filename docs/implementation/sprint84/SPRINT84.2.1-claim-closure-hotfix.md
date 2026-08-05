# Sprint 84.2.1 — Claim Closure Hotfix

```yaml
sprint: 84.2.1
base: 4d1e7af287992c4a92cca60a3ec0897dd493cdc2
authority_gate_dependency_contract: CLOSED
claim_dispatch_proof_prevalidation: CLOSED
worker_runtime_binding: false
production_effect: NONE
production_ready: false
production_cutover_authorized: false
automatic_repair: false
automatic_reconciliation: false
```

Sprint 84.2 remains a canonical `TASK_CLAIM` command contract plus a replay-safe projection primitive.

Sprint 84.3 manager runtime binding remains deferred.

Canonical `RUN_CREATE` binding remains a separate unresolved architecture item.

No worker, Lua, admission, heartbeat, completion, failure, or requeue behavior changed.
