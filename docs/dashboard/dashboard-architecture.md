# Dashboard-A — Read-only Command Center MVP

## Goal

Dashboard-A exposes a read-only command center for IRONCLAD / Toprak1 authority health after Sprint 8 and Sprint 9 burn-down work.

The dashboard is intentionally observational. It must not mutate truth, append events, approve quarantine items, or write Redis state.

## Scope

Included:

- `hfa-dashboard/backend/app.py`
- `hfa-dashboard/backend/read_models/**`
- `hfa-dashboard/backend/routes/**`
- `hfa-dashboard/frontend/**`
- `docs/dashboard/dashboard-architecture.md`

Excluded:

- `hfa-core/**`
- `hfa-control/**`
- `hfa-worker/**`
- `hfa-agents/**`
- `hfa-semantic/**`

## Backend

The backend is a FastAPI app with GET-only routes:

- `GET /dashboard/health`
- `GET /dashboard/authority`
- `GET /dashboard/authority/heatmap`
- `GET /dashboard/findings`

The backend invokes `scripts/authority_audit.py` as a subprocess and consumes its JSON output. This keeps the dashboard decoupled from runtime/control-plane imports and prevents accidental mutation.

## Frontend

The React MVP renders:

- authority status
- banned/suspicious/allowed counts
- risk score
- top risk heatmap
- recent findings

No approve/reject/action buttons exist in Dashboard-A.

## Running Locally

Backend:

```powershell
pip install fastapi uvicorn
uvicorn app:app --reload --app-dir hfa-dashboard/backend
```

Frontend:

```powershell
cd hfa-dashboard/frontend
npm install
npm run dev
```

Optional backend repo root override:

```powershell
$env:HFA_DASHBOARD_REPO_ROOT="C:\Users\FCY\PycharmProjects\TOPRAK1"
```

## Invariants

- Dashboard endpoints are GET-only.
- Dashboard does not write files.
- Dashboard does not connect to Redis directly.
- Dashboard does not import control-plane mutators.
- Dashboard does not append events.
- Quarantine decisions remain out of scope until a later sprint.
