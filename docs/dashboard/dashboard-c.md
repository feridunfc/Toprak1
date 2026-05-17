# Dashboard-C — Live Read-only Event + Worker Telemetry

## Goal

Dashboard-C completes the first command-center pass with read-only event and
worker/effect telemetry panels.

## Backend Endpoints

New GET-only endpoints:

- `GET /dashboard/events`
- `GET /dashboard/workers`

The read models use a tiny dashboard-local Redis reader. It only issues
read-only commands such as `PING`, `SCAN`, `TYPE`, `LRANGE`, `XREVRANGE`,
`HGETALL`, and `TTL`.

If Redis is unavailable, the endpoints return an empty/unavailable snapshot
instead of failing the dashboard.

## Frontend

The UI adds:

- Event Stream panel
- Worker / Effect Telemetry panel
- Dead-letter key counter

## Invariants

- GET-only endpoints
- no Redis writes
- no event append
- no approve/reject/action buttons
- missing Redis must not break the dashboard
- no runtime/control-plane mutator imports
