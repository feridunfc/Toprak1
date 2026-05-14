
from __future__ import annotations

import os


def get_lineage_ttl_seconds() -> int:
    return int(os.getenv("LINEAGE_TTL_SECONDS", "86400"))


def is_lineage_enabled() -> bool:
    return os.getenv("LINEAGE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
