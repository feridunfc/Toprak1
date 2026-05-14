import pytest

from hfa_worker.fake_executor import FakeExecutor


class LegacyLikeRequest:
    def __init__(self, payload):
        self.payload = payload


@pytest.mark.asyncio
async def test_fake_executor_accepts_legacy_like_request():
    executor = FakeExecutor()
    result = await executor.execute(LegacyLikeRequest({"prompt": "hello world"}))

    assert result.status == "done"
    assert result.provider == "fake"
    assert "FAKE_RESPONSE" in result.output_text
    assert result.tokens_used >= 10
