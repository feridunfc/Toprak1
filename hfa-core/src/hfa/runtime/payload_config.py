
from __future__ import annotations

import os


def get_inline_threshold_bytes() -> int:
    return int(os.getenv("INLINE_THRESHOLD_BYTES", "32000"))


def get_payload_backend() -> str:
    return os.getenv("PAYLOAD_BACKEND", "local").strip().lower()


def get_payload_bucket() -> str:
    return os.getenv("PAYLOAD_BUCKET", "ironclad-payloads").strip()


def get_payload_prefix() -> str:
    return os.getenv("PAYLOAD_PREFIX", "payloads").strip().strip("/")


def get_payload_store_timeout_seconds() -> float:
    return float(os.getenv("PAYLOAD_STORE_TIMEOUT_SECONDS", "5.0"))


def get_payload_store_max_retries() -> int:
    return int(os.getenv("PAYLOAD_STORE_MAX_RETRIES", "3"))


def get_payload_store_base_backoff_seconds() -> float:
    return float(os.getenv("PAYLOAD_STORE_BASE_BACKOFF_SECONDS", "0.2"))
