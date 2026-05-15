"""Read-only Dashboard-A FastAPI app."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.authority import router as authority_router
from routes.findings import router as findings_router
from routes.health import router as health_router

app = FastAPI(title="IRONCLAD Dashboard-A", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(authority_router)
app.include_router(findings_router)