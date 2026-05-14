
# Faz 1.1 — Authority Hardening

## Adds
- worker-id binding check in claim
- stronger reaper (`state is None` also orphan)
- safer default reservation TTL (30s)
- concurrency race test for same worker reservation
