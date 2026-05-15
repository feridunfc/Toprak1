# Authority Audit + Deterministic Gates

Sprint 8 adds deterministic, read-only authority checks before dashboard work.
It is a **scanner**, not a production proof engine: it finds direct mutation
risk and feeds CI/dashboard review surfaces.

## Purpose

> Are there direct mutation paths that could bypass event-gated authority?

The audit does **not** import runtime modules, connect to Redis, or write files.

## Tools

### `scripts/authority_audit.py`

AST + Lua source scanner.

```powershell
python scripts/authority_audit.py --repo-root . --format text
python scripts/authority_audit.py --repo-root . --format dashboard
python scripts/authority_audit.py --repo-root . --heatmap
python scripts/authority_audit.py --repo-root . --strict
```

Severity model:

- `allowed`: known authority path or event append path.
- `suspicious`: write-like mutation that needs review.
- `banned`: state-like or critical-runtime mutation without visible safe authority.

Critical runtime files are scanned but not modified:

- `hfa-core/src/hfa/runtime/state_store.py`
- `hfa-control/src/hfa_control/scheduler_loop.py`
- `hfa-worker/src/hfa_worker/runtime/worker_runtime.py`

In critical files, mutation calls default to `banned` unless nearby source contains
a safe authority token such as `AuthoritativeEventGate`, `transition_state`, or
`append_before_authoritative_write`.

Dashboard JSON contains:

- `authority_status`
- `risk_score`
- counts by severity
- finding list with function context
- file-level heatmap

### `scripts/no_direct_write_scanner.py`

Compatibility wrapper around `authority_audit.py`.

### `scripts/full_auto_v3_2_smoke.py`

One-command wrapper for the integrated FULL AUTO v3.2 smoke set.

```powershell
python scripts/full_auto_v3_2_smoke.py --dry-run
python scripts/full_auto_v3_2_smoke.py --redis-mode existing
python scripts/full_auto_v3_2_smoke.py --redis-mode fakeredis
```

`--redis-mode auto` uses Redis at `127.0.0.1:6389` when reachable and otherwise
sets fakeredis-compatible environment variables. Repositories whose fixtures do
not honor `USE_FAKE_REDIS` still need a Redis service for full integration tests.

## CI Gate

`.github/workflows/ci-authority-gate.yml` runs:

1. strict authority audit
2. no-direct-write tests
3. replay compare enforcement tests
4. smoke dry run

## Invariants

- Audit scripts must not modify files.
- Audit scripts must not connect to production services.
- Banned findings must return non-zero exit.
- Strict mode treats suspicious findings as non-zero.
- Dashboard consumes audit JSON; dashboard must not become an authority writer.
