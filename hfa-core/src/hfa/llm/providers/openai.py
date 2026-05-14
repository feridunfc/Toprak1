"""
hfa-core/src/hfa/llm/providers/openai.py
IRONCLAD — OpenAI provider with native structured outputs (response_format)
"""

import json
import logging
from typing import Optional, Type, TypeVar

from openai import AsyncOpenAI
from pydantic import BaseModel, ValidationError

from hfa.core.config import (
    settings,
)  # NOTE: keep as-is if your config module is here; update if needed

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class OpenAIProvider:
    """OpenAI provider with native structured outputs and strict schema validation."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-4o-2024-08-06",
        timeout_seconds: int = 30,
        max_retries: int = 3,
    ):
        self.api_key = api_key or settings.OPENAI_API_KEY
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not set")

        self.model = model
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

        self.client = AsyncOpenAI(
            api_key=self.api_key, timeout=timeout_seconds, max_retries=max_retries
        )
        logger.info(f"OpenAIProvider initialized with model={model}")

    async def generate_structured(
        self,
        prompt: str,
        response_model: Type[T],
        system_prompt: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
    ) -> T:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        schema = response_model.model_json_schema()

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": response_model.__name__,
                    "schema": schema,
                    "strict": True,
                },
            },
        )

        content = response.choices[0].message.content
        if not content:
            raise ValueError("Empty response from OpenAI")

        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e} | content_head={content[:200]}")
            raise ValueError(f"Invalid JSON response: {e}") from e

        # IMPORTANT: propagate ValidationError so RobustLLMClient can retry-on-validation
        try:
            validated = response_model.model_validate(data)
        except ValidationError as e:
            logger.error(f"Validation failed for {response_model.__name__}: {e}")
            raise

        if response.usage:
            logger.info(
                f"Token usage: {response.usage.total_tokens} total "
                f"({response.usage.prompt_tokens} prompt, {response.usage.completion_tokens} completion)"
            )
        return validated

    async def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: Optional[int] = None,
    ) -> str:
        """
        Generate free-form text (non-structured).

        This is required for Researcher-style summarization where strict JSON
        schemas are not desired.

        Returns:
            The model's message content (string).

        Raises:
            ValueError on empty response, Exception on API errors.
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        content = response.choices[0].message.content
        if not content:
            raise ValueError("Empty response from OpenAI")

        if response.usage:
            logger.info(
                "Token usage: %s total (%s prompt, %s completion)",
                response.usage.total_tokens,
                response.usage.prompt_tokens,
                response.usage.completion_tokens,
            )

        return content


async def close(self):
    await self.client.close()
    logger.info("OpenAI client closed")
