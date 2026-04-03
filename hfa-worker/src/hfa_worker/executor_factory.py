"""
hfa-worker/src/hfa_worker/executor_factory.py
IRONCLAD Sprint 1 — Executor Factory (Canonical)

Builds a BaseExecutor from config or environment.

Supported modes
---------------
  fake      : FakeExecutor (default, for tests / local dev)
  openai    : OpenAIExecutor (requires OPENAI_API_KEY)
  cognitive : CognitiveExecutor (requires hfa-semantic + hfa-agents)

Configuration
-------------
Pass a dict or object with the following keys:
  executor_mode           : "fake" | "openai" | "cognitive"
  openai_api_key          : str (openai mode)
  openai_model            : str (openai mode, default "gpt-4o-mini")
  executor_timeout_seconds: float (openai mode, default 60.0)
  cognitive_budget_cents  : int (cognitive mode, default 2000)
  redis_url               : str (cognitive mode)

Environment fallbacks
---------------------
  EXECUTOR_MODE      : overrides executor_mode if not in config
  OPENAI_API_KEY     : overrides openai_api_key if not in config
  COGNITIVE_BUDGET_CENTS

Public API
----------
  build_executor(config) -> BaseExecutor
"""

from __future__ import annotations

import logging
import os
from typing import Any

from hfa_worker.executor import BaseExecutor
from hfa_worker.fake_executor import FakeExecutor

logger = logging.getLogger(__name__)


def _cfg(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def build_executor(config: Any = None) -> BaseExecutor:
    """
    Build and return a canonical BaseExecutor.

    Parameters
    ----------
    config : dict | object | None
        Configuration source. Falls back to environment variables if None
        or if keys are absent.

    Returns
    -------
    BaseExecutor
        A fully initialised executor ready to call .execute(run_event).

    Raises
    ------
    ValueError
        If mode is "openai" and OPENAI_API_KEY is missing, or if an
        unsupported executor_mode is requested.
    """
    if config is None:
        config = {}

    mode = str(
        _cfg(config, "executor_mode", os.getenv("EXECUTOR_MODE", "fake"))
    ).lower()

    logger.info("Building executor: mode=%s", mode)

    # ── fake ──────────────────────────────────────────────────────────────────
    if mode == "fake":
        return FakeExecutor()

    # ── openai ────────────────────────────────────────────────────────────────
    if mode == "openai":
        from hfa_worker.openai_executor import OpenAIExecutor

        api_key = _cfg(config, "openai_api_key") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OPENAI_API_KEY is missing in config and environment (Fail-Fast)."
            )

        model = str(_cfg(config, "openai_model", "gpt-4o-mini"))
        timeout = float(_cfg(config, "executor_timeout_seconds", 60.0))

        executor = OpenAIExecutor(api_key=api_key, model=model, timeout_seconds=timeout)
        logger.info("OpenAIExecutor built: model=%s timeout=%.1fs", model, timeout)
        return executor

    # ── cognitive ─────────────────────────────────────────────────────────────
    if mode == "cognitive":
        from hfa_worker.cognitive_executor import CognitiveExecutor

        executor = CognitiveExecutor.build(config)
        logger.info(
            "CognitiveExecutor built: budget=%s¢",
            _cfg(config, "cognitive_budget_cents", 2000),
        )
        return executor

    raise ValueError(
        f"Unsupported executor_mode: {mode!r}. "
        f"Supported modes: fake, openai, cognitive"
    )
