# hfa-control/Dockerfile
# IRONCLAD OS — Control Plane Image

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

FROM base AS builder

COPY hfa-core/ ./hfa-core/
RUN pip install -e hfa-core

COPY hfa-control/ ./hfa-control/
RUN pip install -e "hfa-control[server]"

# Optional: mount HITL router
COPY ironclad-os/integration/hitl_router.py \
    /app/hfa-control/src/hfa_control/api/hitl_router.py

FROM base AS runtime

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /app /app

WORKDIR /app

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
    CMD curl -f http://localhost:8000/health/live || exit 1

CMD ["uvicorn", "hfa_control.main:build_app", \
     "--factory", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--log-level", "info"]
