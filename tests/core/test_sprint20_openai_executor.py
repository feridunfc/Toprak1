"""
tests/core/test_sprint20_openai_executor.py

Sprint 8: imports migrated from execution_types to canonical locations.
Tests updated to use the canonical ExecutionResult API (payload dict, status,
tokens_used) instead of the old API (output_text, usage, raw_response).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Sprint 8: canonical imports
from hfa.events.schema import RunRequestedEvent
from hfa_worker.models import (
    ExecutionProviderError,
    ExecutionRateLimitError,
    ExecutionTransientError,
)
from hfa_worker.openai_executor import OpenAIExecutor


def _make_event(prompt: str = "Say hello", run_id: str = "run-1") -> RunRequestedEvent:
    return RunRequestedEvent(
        run_id=run_id,
        tenant_id="tenant-1",
        agent_type="openai",
        payload={"prompt": prompt},
    )


@pytest.fixture
def mock_openai_client():
    with patch("hfa_worker.openai_executor.AsyncOpenAI") as mock_client_class:
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.mark.asyncio
async def test_success(mock_openai_client):
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Hello there!"
    mock_response.usage = MagicMock(
        prompt_tokens=10, completion_tokens=5, total_tokens=15
    )
    mock_response.model = "gpt-4"
    mock_response.model_dump.return_value = {"id": "resp-1"}
    mock_openai_client.chat.completions.create.return_value = mock_response

    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4", timeout_seconds=30.0)
    result = await executor.execute(_make_event())

    assert result.status == "done"
    assert result.payload["output_text"] == "Hello there!"
    assert result.payload["provider"] == "openai"
    assert result.payload["model"] == "gpt-4"
    assert result.payload["prompt_tokens"] == 10
    assert result.payload["completion_tokens"] == 5
    assert result.tokens_used == 15


@pytest.mark.asyncio
async def test_missing_prompt_returns_failed():
    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")
    event = RunRequestedEvent(
        run_id="run-1", tenant_id="tenant-1", agent_type="openai", payload={}
    )
    result = await executor.execute(event)
    assert result.status == "failed"
    assert "prompt" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_empty_choices_is_transient(mock_openai_client):
    mock_response = MagicMock()
    mock_response.choices = []
    mock_openai_client.chat.completions.create.return_value = mock_response

    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")
    result = await executor.execute(_make_event())
    assert result.status == "failed"
    assert result.payload.get("retryable") is True


@pytest.mark.asyncio
async def test_rate_limit_returns_failed_retryable(mock_openai_client):
    from openai import RateLimitError
    mock_openai_client.chat.completions.create.side_effect = RateLimitError(
        message="Too many requests", response=MagicMock(), body=None,
    )
    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")
    result = await executor.execute(_make_event())
    assert result.status == "failed"
    assert result.payload.get("retryable") is True


@pytest.mark.asyncio
async def test_bad_request_returns_failed(mock_openai_client):
    from openai import BadRequestError
    mock_openai_client.chat.completions.create.side_effect = BadRequestError(
        message="Bad request", response=MagicMock(), body=None,
    )
    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")
    result = await executor.execute(_make_event())
    assert result.status == "failed"
    assert result.payload.get("retryable") is None  # permanent error, not retryable
