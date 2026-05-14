"""
hfa-control/src/hfa_control/effect_config.py
IRONCLAD Phase 7C — Effect TTL configuration.

TTL'ler artık hardcoded değil; env/config üzerinden okunur.
Varsayılanlar: dispatch=1h, completion=24h.
"""
from __future__ import annotations

import os


def get_dispatch_effect_ttl() -> int:
    """
    Dispatch effect key TTL (seconds).
    Env: DISPATCH_EFFECT_TTL_SECONDS
    Default: 3600 (1 hour)
    """
    return int(os.environ.get("DISPATCH_EFFECT_TTL_SECONDS", "3600"))


def get_completion_effect_ttl() -> int:
    """
    Completion effect key TTL (seconds).
    Env: COMPLETION_EFFECT_TTL_SECONDS
    Default: 86400 (24 hours)
    """
    return int(os.environ.get("COMPLETION_EFFECT_TTL_SECONDS", "86400"))
