# hfa-worker/Dockerfile
# IRONCLAD OS — Cognitive Worker Image

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Build stage: install all packages ─────────────────────────────────────
FROM base AS builder

# Install hfa-core first (dependency of everything)
COPY hfa-core/ ./hfa-core/
RUN pip install -e hfa-core

# Install hfa-control (for task_claim, models)
COPY hfa-control/ ./hfa-control/
RUN pip install -e hfa-control

# Install hfa-worker
COPY hfa-worker/ ./hfa-worker/
RUN pip install -e hfa-worker

# Install hfa-semantic (optional — worker degrades if absent)
COPY hfa-semantic/ ./hfa-semantic/ 2>/dev/null || true
RUN pip install -e hfa-semantic[redis] 2>/dev/null || \
    echo "hfa-semantic not found — cognitive worker will run in degraded mode"

# Install hfa-agents
COPY hfa-agents/ ./hfa-agents/ 2>/dev/null || true
RUN pip install -e "hfa-agents[anthropic,openai,docker]" 2>/dev/null || \
    echo "hfa-agents not found — cognitive worker disabled"

# ── Runtime stage ──────────────────────────────────────────────────────────
FROM base AS runtime

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /app /app

# Copy integration files (new files, not changing sealed ones)
COPY ironclad-os/integration/cognitive_executor.py   /app/hfa-worker/src/hfa_worker/
COPY ironclad-os/integration/feedback_writer.py      /app/hfa-worker/src/hfa_worker/
COPY ironclad-os/integration/executor_factory_patch.py /app/hfa-worker/src/hfa_worker/executor_factory.py

WORKDIR /app

# Health check: verify worker can start
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD python -c "from hfa_worker.executor_factory import build_executor; print('ok')"

# Default: cognitive mode
ENV EXECUTOR_MODE=cognitive \
    LOG_LEVEL=INFO

CMD ["python", "-m", "hfa_worker.main"]
