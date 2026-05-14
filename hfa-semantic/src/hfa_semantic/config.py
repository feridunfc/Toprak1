"""
hfa-semantic/src/hfa_semantic/config.py

Fix: replaced pydantic_settings (optional dep) with plain os.getenv.
Same interface — settings.SEMANTIC_ENABLED etc. all work identically.
"""
from __future__ import annotations
import os


class SemanticSettings:
    """Semantic layer configuration from environment variables."""

    @property
    def SEMANTIC_ENABLED(self) -> bool:
        return os.getenv("SEMANTIC_ENABLED", "true").lower() not in ("false", "0", "no")

    @property
    def SEMANTIC_STATE_BACKEND(self) -> str:
        return os.getenv("SEMANTIC_STATE_BACKEND", "redis")

    @property
    def SEMANTIC_REDIS_URL(self) -> str | None:
        return os.getenv("SEMANTIC_REDIS_URL")

    @property
    def SEMANTIC_MAX_PARTITIONS_PER_RULE(self) -> int:
        return int(os.getenv("SEMANTIC_MAX_PARTITIONS_PER_RULE", "10000"))

    @property
    def SEMANTIC_STATE_TTL_MS(self) -> int:
        return int(os.getenv("SEMANTIC_STATE_TTL_MS", "3600000"))

    @property
    def SEMANTIC_ALLOWED_LATENESS_MS(self) -> int:
        return int(os.getenv("SEMANTIC_ALLOWED_LATENESS_MS", "10000"))

    @property
    def SEMANTIC_LATE_EVENT_POLICY(self) -> str:
        return os.getenv("SEMANTIC_LATE_EVENT_POLICY", "drop")

    @property
    def SEMANTIC_DEDUP_TTL_MS(self) -> int:
        return int(os.getenv("SEMANTIC_DEDUP_TTL_MS", "3600000"))

    @property
    def SEMANTIC_METRICS_PORT(self) -> int:
        return int(os.getenv("SEMANTIC_METRICS_PORT", "8000"))

    @property
    def SEMANTIC_LOG_LEVEL(self) -> str:
        return os.getenv("SEMANTIC_LOG_LEVEL", "INFO")

    @property
    def SEMANTIC_PARTITION_STRATEGY(self) -> str:
        return os.getenv("SEMANTIC_PARTITION_STRATEGY", "entity_id")


settings = SemanticSettings()
