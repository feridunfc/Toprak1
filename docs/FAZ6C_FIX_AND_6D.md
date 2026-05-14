
# Faz 6C Fix + Faz 6D — Backpressure & Load Shedding

## 6C Fix
Chaos tests now include a local `redis_client` fixture inside `tests/chaos/conftest.py`.
It prefers `fakeredis` when available and falls back to `redis.asyncio`.

This fixes:
- missing `redis_client` fixture in chaos tests

## 6D Goal
Prevent scheduler overload and noisy-tenant domination.

## What this pack adds
- backpressure policy
- load shedding decisions
- tenant pending limit
- global pending limit
- tenant running limit
- core + integration tests

## Suggested commands
```bash
pytest tests/chaos/ -vv
pytest tests/core/test_backpressure_policy_core.py -vv
pytest tests/integration/test_load_shedder_integration.py -vv
```
