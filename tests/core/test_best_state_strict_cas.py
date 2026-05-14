
import os
from unittest.mock import AsyncMock, patch

import pytest

from hfa.state import transition_state


@pytest.mark.asyncio
async def test_strict_env_requires_lua():
    redis = AsyncMock()
    with patch.dict(os.environ, {"APP_ENV": "production"}, clear=True):
        with pytest.raises(RuntimeError):
            await transition_state(
                redis,
                run_id="r1",
                expected_state="running",
                target_state="rescheduled",
                ttl_seconds=60,
                lua_loader=None,
            )


@pytest.mark.asyncio
async def test_non_strict_env_allows_python_fallback():
    redis = AsyncMock()
    redis.get.return_value = b"running"
    with patch.dict(os.environ, {"APP_ENV": "development"}, clear=True):
        result = await transition_state(
            redis,
            run_id="r1",
            expected_state="running",
            target_state="rescheduled",
            ttl_seconds=60,
            lua_loader=None,
        )
        assert result.ok is True
        redis.set.assert_awaited_once()
