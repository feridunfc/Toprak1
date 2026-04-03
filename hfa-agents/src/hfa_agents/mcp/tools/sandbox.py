"""
hfa-agents/src/hfa_agents/mcp/tools/sandbox.py

IRONCLAD Sprint 1 — Sandbox Tool (Production-Safe)

Execution order:
  1. If hfa_tools.sandbox.SandboxRunner is importable → use it (real sandbox).
  2. Otherwise → safe local fallback (no execution, returns stub result).

Safety guards applied in both paths:
  * Code size limit (MAX_CODE_BYTES) — reject oversized submissions.
  * Language allowlist (ALLOWED_LANGUAGES) — reject unknown languages.
  * Fallback never executes code — it returns a safe stub so callers
    (e.g. CoderAgent) get a valid result dict without running untrusted code.

The public interface is unchanged:
    await SandboxTool().execute(code, language="python") → dict
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Safety constants ──────────────────────────────────────────────────────────

# Maximum code size accepted. Submissions larger than this are rejected
# before reaching the sandbox to prevent resource exhaustion.
MAX_CODE_BYTES: int = 64 * 1024  # 64 KB

# Languages the sandbox is allowed to execute.
# Extend this list when new sandbox runtimes are added and validated.
ALLOWED_LANGUAGES: frozenset[str] = frozenset({
    "python",
    "javascript",
    "typescript",
    "bash",
    "sh",
})

# ── Real sandbox loader ───────────────────────────────────────────────────────

def _try_load_real_sandbox() -> Any:
    """
    Attempt to import hfa_tools.sandbox.SandboxRunner.

    Returns the class on success, None if the package is not installed.
    This is called once at module import time so the import cost is paid
    only once per worker process.
    """
    try:
        from hfa_tools.sandbox import SandboxRunner  # type: ignore[import]
        logger.info("SandboxTool: hfa_tools.sandbox.SandboxRunner available")
        return SandboxRunner
    except ImportError:
        logger.info(
            "SandboxTool: hfa_tools not installed — using safe fallback (no execution)"
        )
        return None


_RealSandboxRunner = _try_load_real_sandbox()


# ── SandboxTool ───────────────────────────────────────────────────────────────

class SandboxTool:
    """
    Executes code in an isolated sandbox (if available) or returns a safe
    stub result (if sandbox runtime is not installed).

    Public API:
        await SandboxTool().execute(code, language="python") → dict
    """

    async def execute(self, code: str, language: str = "python") -> dict:
        """
        Execute code in the sandbox.

        Args:
            code:     Source code to execute.
            language: Target runtime. Must be in ALLOWED_LANGUAGES.

        Returns:
            dict with keys: success, exit_code, stdout, stderr, language,
            size_bytes, sandbox_mode ("real" | "fallback" | "rejected").
        """
        size_bytes = len(code.encode("utf-8"))

        # ── Guard: language allowlist ─────────────────────────────────────
        if language not in ALLOWED_LANGUAGES:
            logger.warning(
                "SandboxTool: rejected unsupported language=%s", language
            )
            return {
                "success":      False,
                "exit_code":    1,
                "stdout":       "",
                "stderr":       f"unsupported_language:{language}",
                "language":     language,
                "size_bytes":   size_bytes,
                "sandbox_mode": "rejected",
            }

        # ── Guard: code size ──────────────────────────────────────────────
        if size_bytes > MAX_CODE_BYTES:
            logger.warning(
                "SandboxTool: rejected code size=%d bytes (limit=%d)",
                size_bytes,
                MAX_CODE_BYTES,
            )
            return {
                "success":      False,
                "exit_code":    1,
                "stdout":       "",
                "stderr":       f"code_too_large:{size_bytes}_bytes",
                "language":     language,
                "size_bytes":   size_bytes,
                "sandbox_mode": "rejected",
            }

        # ── Real sandbox ──────────────────────────────────────────────────
        if _RealSandboxRunner is not None:
            return await self._execute_real(code, language, size_bytes)

        # ── Safe fallback ─────────────────────────────────────────────────
        return self._execute_fallback(code, language, size_bytes)

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    async def _execute_real(code: str, language: str, size_bytes: int) -> dict:
        """
        Delegate to the real hfa_tools SandboxRunner.

        Wraps the call so that any runner exception is caught and returned
        as a failed result — callers must never see an unhandled exception
        from the sandbox layer.
        """
        try:
            runner = _RealSandboxRunner()
            result = await runner.run(code=code, language=language)
            # Normalise result to the expected dict shape
            if isinstance(result, dict):
                result.setdefault("sandbox_mode", "real")
                result.setdefault("size_bytes", size_bytes)
                result.setdefault("language", language)
                return result
            # Non-dict result — wrap it
            return {
                "success":      True,
                "exit_code":    0,
                "stdout":       str(result),
                "stderr":       "",
                "language":     language,
                "size_bytes":   size_bytes,
                "sandbox_mode": "real",
            }
        except Exception as exc:
            logger.error("SandboxTool: real sandbox raised: %s", exc, exc_info=True)
            return {
                "success":      False,
                "exit_code":    1,
                "stdout":       "",
                "stderr":       f"sandbox_error:{type(exc).__name__}:{exc}",
                "language":     language,
                "size_bytes":   size_bytes,
                "sandbox_mode": "real",
            }

    @staticmethod
    def _execute_fallback(code: str, language: str, size_bytes: int) -> dict:
        """
        Safe stub result — does NOT execute code.

        Used when hfa_tools is not installed. Returns a successful-looking
        result so that callers (CoderAgent etc.) can continue their pipeline
        without crashing, while the sandbox_mode="fallback" field signals
        that no real execution happened.
        """
        logger.debug(
            "SandboxTool: fallback mode — code not executed (size=%d bytes, lang=%s)",
            size_bytes,
            language,
        )
        return {
            "success":      True,
            "exit_code":    0,
            "stdout":       "sandbox_fallback_ok",
            "stderr":       "",
            "language":     language,
            "size_bytes":   size_bytes,
            "sandbox_mode": "fallback",
        }
