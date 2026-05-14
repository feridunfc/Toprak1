import os
from unittest.mock import patch

import pytest

from hfa_worker.executor_factory import build_executor


def test_build_executor_openai_mode_fail_fast_missing_key():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="OPENAI_API_KEY is missing"):
            build_executor({"executor_mode": "openai"})


def test_build_executor_openai_mode_success_with_dict():
    with patch.dict(os.environ, {}, clear=True):
        executor = build_executor(
            {
                "executor_mode": "openai",
                "openai_api_key": "sk-test-key",
                "openai_model": "gpt-4",
                "executor_timeout_seconds": 30.0,
            }
        )

    assert type(executor).__name__ == "OpenAIExecutor"
    assert getattr(executor, "_model", None) == "gpt-4"
    assert getattr(executor, "_timeout_seconds", None) == 30.0


def test_build_executor_openai_mode_success_with_env():
    with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-env-key"}, clear=True):
        executor = build_executor({"executor_mode": "openai"})

    assert type(executor).__name__ == "OpenAIExecutor"