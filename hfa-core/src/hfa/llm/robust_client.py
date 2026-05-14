"""
hfa-core/src/hfa/llm/robust_client.py
IRONCLAD — Robust client with retry, circuit breaker, and validation
"""

import asyncio
import logging
from typing import Optional, Type, TypeVar

from pydantic import BaseModel, ValidationError

from hfa.healing.circuit_breaker import CircuitBreaker, CircuitOpenError
from hfa.llm.providers.openai import OpenAIProvider

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class LLMCallError(Exception):
    """Base exception for LLM call failures."""


class LLMTimeoutError(LLMCallError):
    """LLM call timed out."""


class LLMValidationError(LLMCallError):
    """LLM response failed validation."""


class RobustLLMClient:
    """LLM client with retry/backoff + circuit breaker + schema validation."""

    def __init__(
        self,
        provider: str = "openai",
        default_model: str = "gpt-4o-2024-08-06",
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        circuit_failure_threshold: int = 5,
        circuit_recovery_timeout: int = 60,
        request_timeout: int = 30,
    ):
        self.provider_name = provider
        self.default_model = default_model
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.request_timeout = request_timeout

        self._circuits: dict[str, CircuitBreaker] = {}
        self._circuit_failure_threshold = circuit_failure_threshold
        self._circuit_recovery_timeout = circuit_recovery_timeout

        self._provider = self._init_provider(provider)

        logger.info(f"RobustLLMClient initialized with provider={provider}")

    def _init_provider(self, provider: str):
        if provider == "openai":
            return OpenAIProvider(
                model=self.default_model,
                timeout_seconds=self.request_timeout,
                max_retries=0,
            )
        raise ValueError(f"Unsupported provider: {provider}")

    def _get_circuit(self, model: str) -> CircuitBreaker:
        if model not in self._circuits:
            self._circuits[model] = CircuitBreaker(
                name=f"llm-{model}",
                failure_threshold=self._circuit_failure_threshold,
                recovery_timeout=self._circuit_recovery_timeout,
            )
        return self._circuits[model]

    async def generate_structured(
        self,
        prompt: str,
        response_model: Type[T],
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        retry_on_validation: bool = True,
    ) -> T:
        model = model or self.default_model
        circuit = self._get_circuit(model)

        if circuit.is_open():
            logger.warning(f"Circuit open for model {model}, failing fast")
            raise CircuitOpenError(f"Circuit open for model {model}")

        last_exception: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                enhanced_prompt = prompt
                if attempt > 0:
                    enhanced_prompt = (
                        f"{prompt}\n\n"
                        "Previous attempt failed. Return STRICTLY valid JSON matching the schema. "
                        f"Error: {last_exception}"
                    )

                result = await asyncio.wait_for(
                    self._provider.generate_structured(
                        prompt=enhanced_prompt,
                        response_model=response_model,
                        system_prompt=system_prompt,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    ),
                    timeout=self.request_timeout,
                )

                circuit.record_success()
                return result

            except asyncio.TimeoutError as e:
                last_exception = e
                logger.warning(
                    f"LLM timeout (attempt {attempt + 1}/{self.max_retries + 1})"
                )
                circuit.record_failure()

            except ValidationError as e:
                last_exception = e
                logger.warning(
                    f"Validation failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}"
                )
                # do NOT record circuit failure for validation drift; it's not provider outage
                if (not retry_on_validation) or (attempt == self.max_retries):
                    raise LLMValidationError(f"Response validation failed: {e}") from e

            except CircuitOpenError:
                raise

            except Exception as e:
                last_exception = e
                logger.warning(
                    f"LLM call failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}"
                )
                circuit.record_failure()

            if attempt < self.max_retries:
                delay = min(self.base_delay * (2**attempt), self.max_delay)
                logger.info(f"Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)

        circuit.record_failure()
        raise LLMCallError(
            f"All {self.max_retries + 1} attempts failed. Last error: {last_exception}"
        )

    async def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: Optional[int] = None,
    ) -> str:
        """
        Generate free-form text with retry + circuit breaker.

        This complements generate_structured() for cases where we intentionally
        want non-JSON output (e.g., research summaries).

        Raises:
            CircuitOpenError, LLMTimeoutError, LLMCallError
        """
        model = model or self.default_model
        circuit = self._get_circuit(model)

        if circuit.is_open():
            logger.warning("Circuit open for model %s, failing fast", model)
            raise CircuitOpenError(f"Circuit open for model {model}")

        last_exception: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                enhanced_prompt = prompt
                if attempt > 0 and last_exception is not None:
                    enhanced_prompt = (
                        f"{prompt}\n\nPrevious attempt failed. "
                        f"Please answer clearly. Error: {last_exception}"
                    )

                result = await asyncio.wait_for(
                    self._provider.generate_text(
                        prompt=enhanced_prompt,
                        system_prompt=system_prompt,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    ),
                    timeout=self.request_timeout,
                )

                circuit.record_success()
                return result

            except asyncio.TimeoutError as e:
                last_exception = e
                logger.warning(
                    "LLM text timeout (attempt %s/%s)",
                    attempt + 1,
                    self.max_retries + 1,
                )
                circuit.record_failure()

            except Exception as e:
                last_exception = e
                logger.warning(
                    "LLM text call failed (attempt %s/%s): %s",
                    attempt + 1,
                    self.max_retries + 1,
                    e,
                )
                circuit.record_failure()

            if attempt < self.max_retries:
                delay = min(self.base_delay * (2**attempt), self.max_delay)
                logger.info("Retrying in %.2fs...", delay)
                await asyncio.sleep(delay)

        circuit.record_failure()
        raise LLMCallError(
            f"All {self.max_retries + 1} attempts failed. Last error: {last_exception}"
        )


async def close(self):
    if hasattr(self._provider, "close"):
        await self._provider.close()
    logger.info("RobustLLMClient closed")
