# IRONCLAD v3 Config Matrix

## Semantic modes

| Setting | Default | Meaning | Release rule |
|---|---:|---|---|
| `IRON_SEMANTIC_GATE_MODE=legacy` | yes | Semantic checks are advisory only. Failures may degrade/fail open. | Safe rollback mode. |
| `IRON_SEMANTIC_GATE_MODE=advisory` | no | Explicit advisory mode. Verdicts are observable but not authoritative. | Suitable for shadow/canary. |
| `IRON_SEMANTIC_GATE_MODE=gate` | no | Semantic gate is authoritative and must fail closed. | Use only after replay/audit visibility is verified. |
| `IRON_SEMANTIC_GATE_MODE=strict` | no | Alias for v3 fail-closed gate behavior. | Production release mode after canary. |

## Constitutional rules

- Advisory semantics may fail open, but must remain observable.
- Gate semantics must fail closed.
- Gate verdicts must be replay-visible or audit-visible.
- Scheduler core authority is not replaced by semantic checks.
- Worker executor internals are not changed by semantic release mode.
