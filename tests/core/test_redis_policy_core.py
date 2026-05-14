
import pytest

from hfa.runtime.failure_semantics import DistributedError, FailureCategory
from hfa.runtime.redis_policy import RedisCallPolicy

pytestmark = pytest.mark.asyncio


async def test_retries_transient_then_succeeds():
    policy = RedisCallPolicy(max_retries=3, base_delay_ms=1, jitter_ms=0)
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise Exception("timeout talking to redis")
        return "ok"

    result = await policy.execute_with_policy("op", op)
    assert result == "ok"
    assert calls["n"] == 3


async def test_terminal_error_fails_fast():
    policy = RedisCallPolicy(max_retries=3, base_delay_ms=1, jitter_ms=0)

    async def op():
        raise Exception("NOSCRIPT no such script")

    with pytest.raises(DistributedError) as exc:
        await policy.execute_with_policy("op", op)

    assert exc.value.category == FailureCategory.TERMINAL


async def test_unknown_error_retries_until_exhausted():
    policy = RedisCallPolicy(max_retries=2, base_delay_ms=1, jitter_ms=0)
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        raise Exception("weird unexplained failure")

    with pytest.raises(DistributedError) as exc:
        await policy.execute_with_policy("op", op)

    assert exc.value.category == FailureCategory.UNKNOWN
    assert calls["n"] == 3
