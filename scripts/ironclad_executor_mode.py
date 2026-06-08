from __future__ import annotations

import os
from typing import Any

DEFAULT_EXECUTOR_MODE = "fake"
FAKE_MODE = "fake"
PRODUCTION_BLOCKED_MODE = "production_llm_blocked"
PRODUCTION_ENABLED_MODE = "production_llm_enabled"

VALID_EXECUTOR_MODES = {
    FAKE_MODE,
    PRODUCTION_BLOCKED_MODE,
    PRODUCTION_ENABLED_MODE,
}


def requested_executor_mode(env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    mode = (source.get("IRONCLAD_EXECUTOR_MODE") or DEFAULT_EXECUTOR_MODE).strip().lower()
    if mode in {"", "default"}:
        return DEFAULT_EXECUTOR_MODE
    if mode in {"openai", "anthropic", "production", "real", "llm"}:
        return PRODUCTION_ENABLED_MODE
    if mode not in VALID_EXECUTOR_MODES:
        return DEFAULT_EXECUTOR_MODE
    return mode


def real_llm_allowed(env: dict[str, str] | None = None) -> bool:
    source = env if env is not None else os.environ
    return (source.get("IRONCLAD_ALLOW_REAL_LLM") or "").strip() == "1"


def executor_mode_boundary(env: dict[str, str] | None = None) -> dict[str, Any]:
    mode = requested_executor_mode(env)
    allow_real = real_llm_allowed(env)

    if mode == PRODUCTION_ENABLED_MODE and not allow_real:
        return {
            "executor_mode": PRODUCTION_BLOCKED_MODE,
            "requested_executor_mode": mode,
            "fake_executor_used": False,
            "production_llm_call_attempted": False,
            "real_llm_blocked": True,
            "blocked_reason": "real LLM execution requires IRONCLAD_ALLOW_REAL_LLM=1",
        }

    if mode == PRODUCTION_ENABLED_MODE and allow_real:
        return {
            "executor_mode": PRODUCTION_ENABLED_MODE,
            "requested_executor_mode": mode,
            "fake_executor_used": False,
            "production_llm_call_attempted": False,
            "real_llm_blocked": False,
            "blocked_reason": None,
        }

    if mode == PRODUCTION_BLOCKED_MODE:
        return {
            "executor_mode": PRODUCTION_BLOCKED_MODE,
            "requested_executor_mode": mode,
            "fake_executor_used": False,
            "production_llm_call_attempted": False,
            "real_llm_blocked": True,
            "blocked_reason": "production LLM execution is blocked by executor mode",
        }

    return {
        "executor_mode": FAKE_MODE,
        "requested_executor_mode": mode,
        "fake_executor_used": True,
        "production_llm_call_attempted": False,
        "real_llm_blocked": False,
        "blocked_reason": None,
    }


def apply_executor_mode_boundary(payload: dict[str, Any], env: dict[str, str] | None = None) -> dict[str, Any]:
    boundary = executor_mode_boundary(env)
    merged = dict(payload)

    # Preserve stronger runtime truth when present, but expose normalized mode fields.
    merged["executor_mode"] = boundary["executor_mode"]
    merged["requested_executor_mode"] = boundary["requested_executor_mode"]
    merged["real_llm_blocked"] = boundary["real_llm_blocked"]
    merged["blocked_reason"] = boundary["blocked_reason"]

    if boundary["real_llm_blocked"]:
        merged["fake_executor_used"] = False
        merged["production_llm_call_attempted"] = False
    else:
        merged["fake_executor_used"] = bool(merged.get("fake_executor_used", boundary["fake_executor_used"]))
        merged["production_llm_call_attempted"] = bool(
            merged.get("production_llm_call_attempted", boundary["production_llm_call_attempted"])
        )

    return merged
