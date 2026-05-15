# IRONCLAD v3 Semantic Gate Runbook

## Rollback

Set:

```text
IRON_SEMANTIC_GATE_MODE=legacy
```

This returns semantic checks to advisory behavior.

## Canary flow

1. Run in `legacy` or `advisory` mode.
2. Record semantic verdicts and false positives.
3. Verify verdicts are audit/replay visible.
4. Move a narrow slice to `gate`.
5. Roll back immediately if gate false positives block valid work.

## Incident notes

- Advisory failures should not stop scheduler or worker progress.
- Gate failures must stop the guarded action.
- Missing evaluator in gate mode is treated as deny/fail-closed.
- Semantic verdict payloads must include mode, allowed, reason, and visibility flags.
