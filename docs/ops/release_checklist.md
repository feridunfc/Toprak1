# IRONCLAD v3 Release Checklist

## Semantic constitution

- [ ] Advisory and gate modes are separate.
- [ ] Advisory failures degrade/fail open.
- [ ] Gate failures fail closed.
- [ ] Gate verdicts are audit-visible.
- [ ] Gate verdicts are replay-visible or explicitly persisted for audit.
- [ ] Rollback is documented with `IRON_SEMANTIC_GATE_MODE=legacy`.

## Test commands

```powershell
$env:IRON_SEMANTIC_GATE_MODE="gate"
python -m pytest tests/integration/test_phase5b_strict_mode.py tests/integration/test_phase6a_runtime_safety.py -q --tb=short
```
