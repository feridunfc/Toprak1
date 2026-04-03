"""
hfa-worker/src/hfa_worker/openai_executor.py
IRONCLAD Sprint 1 — OpenAI Executor (Canonical)

Implements BaseExecutor. Calls OpenAI chat completions API and returns
a canonical hfa_worker.models.ExecutionResult.

Input compatibility
-------------------
Accepts both RunRequestedEvent (canonical) and ExecutionRequest (legacy).
Both expose .payload — 'prompt' key is read from payload.

Exception mapping
-----------------
All OpenAI exceptions are caught and returned as ExecutionResult(status="failed").
Retryable errors are logged at WARNING; permanent errors at ERROR.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from openai import (
    APIConnectionError,
    APIError,
    APITimeoutError,
    AsyncOpenAI,
    AuthenticationError,
    BadRequestError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    UnprocessableEntityError,
)

from hfa_worker.executor import BaseExecutor
from hfa_worker.models import (
    ExecutionError,
    ExecutionProviderError,
    ExecutionRateLimitError,
    ExecutionResult,
    ExecutionTimeoutError,
    ExecutionTransientError,
)

logger = logging.getLogger(__name__)


class OpenAIExecutor(BaseExecutor):
    """
    Production OpenAI executor.

    Parameters
    ----------
    api_key         : OpenAI API key (required)
    model           : model identifier (default: gpt-4o-mini)
    timeout_seconds : per-request timeout
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        timeout_seconds: float = 60.0,
    ) -> None:
        if not api_key:
            raise ValueError("OpenAI API key is missing.")
        self._model = model
        self._timeout_seconds = float(timeout_seconds)
        self._client = AsyncOpenAI(api_key=api_key, timeout=self._timeout_seconds)

    async def execute(self, run_event: Any) -> ExecutionResult:
        """
        Execute via OpenAI chat completions.

        Accepts RunRequestedEvent or ExecutionRequest.
        Returns canonical hfa_worker.models.ExecutionResult.
        Never raises — all exceptions are mapped to status="failed".
        """
        payload = getattr(run_event, "payload", {}) or {}
        run_id = getattr(run_event, "run_id", "unknown")
        prompt = payload.get("prompt")

        if not prompt:
            return ExecutionResult(
                status="failed",
                payload={"run_id": run_id},
                error="Missing 'prompt' in request payload.",
                cost_cents=0,
                tokens_used=0,
            )

        start_time = time.monotonic()

        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": str(prompt)}],
            )

            latency_ms = (time.monotonic() - start_time) * 1000.0

            if not getattr(response, "choices", None):
                raise ExecutionTransientError("Empty choices array from OpenAI")

            output_text = getattr(response.choices[0].message, "content", "") or ""

            usage = getattr(response, "usage", None)
            prompt_tokens = getattr(usage, "prompt_tokens", 0) if usage else 0
            completion_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
            total_tokens = getattr(usage, "total_tokens", 0) if usage else 0

            return ExecutionResult(
                status="done",
                payload={
                    "run_id": run_id,
                    "output_text": output_text,
                    "model": getattr(response, "model", self._model),
                    "provider": "openai",
                    "latency_ms": round(latency_ms, 1),
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                },
                cost_cents=0,  # caller may compute from token counts if needed
                tokens_used=total_tokens,
            )

        except APITimeoutError as exc:
            logger.warning("OpenAIExecutor timeout run=%s: %s", run_id, exc)
            return self._transient_failure(run_id, f"timeout: {exc}")

        except RateLimitError as exc:
            logger.warning("OpenAIExecutor rate_limit run=%s: %s", run_id, exc)
            return self._transient_failure(run_id, f"rate_limit: {exc}")

        except APIConnectionError as exc:
            logger.warning("OpenAIExecutor connection_error run=%s: %s", run_id, exc)
            return self._transient_failure(run_id, f"connection_error: {exc}")

        except (
            BadRequestError,
            AuthenticationError,
            PermissionDeniedError,
            NotFoundError,
            UnprocessableEntityError,
        ) as exc:
            logger.error("OpenAIExecutor provider_error run=%s: %s", run_id, exc)
            return ExecutionResult(
                status="failed",
                payload={"run_id": run_id},
                error=f"provider_error: {exc}",
                cost_cents=0,
                tokens_used=0,
            )

        except APIError as exc:
            logger.warning("OpenAIExecutor api_error run=%s: %s", run_id, exc)
            return self._transient_failure(run_id, f"api_error: {exc}")

        except ExecutionError:
            raise  # let caller handle explicitly typed execution errors

        except Exception as exc:
            logger.error("OpenAIExecutor unexpected run=%s: %s", run_id, exc, exc_info=True)
            return ExecutionResult(
                status="failed",
                payload={"run_id": run_id},
                error=f"unexpected_error: {exc}",
                cost_cents=0,
                tokens_used=0,
            )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _transient_failure(run_id: str, reason: str) -> ExecutionResult:
        return ExecutionResult(
            status="failed",
            payload={"run_id": run_id, "retryable": True},
            error=reason,
            cost_cents=0,
            tokens_used=0,
        )
