
import pytest
from unittest.mock import AsyncMock, patch

from hfa.state import transition_state, TransitionResult


@pytest.mark.asyncio
async def test_strict_cas_implied_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    redis = object()
    with patch("hfa.state._get_lua_loader", AsyncMock(side_effect=RuntimeError("boom"))):
        with pytest.raises(RuntimeError):
            await transition_state(redis, "r1", "scheduled", state_key="k", expected_state="queued")


@pytest.mark.asyncio
async def test_unknown_state_code_maps_correctly(monkeypatch):
    monkeypatch.delenv("APP_ENV", raising=False)

    class _Loader:
        async def run(self, **kwargs):
            return [-4, "mystery"]

    with patch("hfa.state._get_lua_loader", AsyncMock(return_value=_Loader())):
        res = await transition_state(object(), "r2", "scheduled", state_key="k", expected_state="queued")
    assert res.reason == TransitionResult.UNKNOWN_STATE
