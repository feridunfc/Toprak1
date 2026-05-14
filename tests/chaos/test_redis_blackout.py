
import pytest

from hfa.runtime.state_store import StateStore

pytestmark = pytest.mark.asyncio


class FlakyLuaExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, *, script_name, keys, args):
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("CHAOS: Network partition")
        return {"script_name": script_name, "keys": keys, "args": args}


class FakeRedis:
    async def incrbyfloat(self, key, delta):
        return 1.0


@pytest.mark.chaos
async def test_scheduler_survives_redis_blackout():
    store = StateStore(redis_client=FakeRedis(), lua_executor=FlakyLuaExecutor())
    result = await store.reserve_worker(worker_id="worker-1", run_id="run-1", ttl_ms=5000)

    assert result["script_name"] == "reserve_worker"
    assert result["keys"] == ["hfa:worker:worker-1:reservation"]
