from __future__ import annotations

import pytest

from hfa.events.completion_capture import LLM_COMPLETION_CAPTURED, replay_captured_completion
from hfa.events.execution_artifacts import seal_execution_artifact


@pytest.mark.asyncio
async def test_replay_reconstructs_inline_completion_without_llm_call():
    sealed = await seal_execution_artifact({"answer": "from-event"}, inline_limit_bytes=1024)

    async def live_llm_call():  # pragma: no cover - must never be called
        raise AssertionError("replay must not call live LLM")

    details = {
        "event_type": LLM_COMPLETION_CAPTURED,
        "details": {
            "provider": "test",
            "model": "none",
            "prompt_hash": "p",
            "system_prompt_hash": "s",
            "output_hash": sealed.content_hash,
            "tokens": 0,
            "cost_cents": 0,
            "fallback_used": False,
            "output": sealed.to_event_payload(),
        },
    }

    output = await replay_captured_completion(details)
    assert output == {"answer": "from-event"}


@pytest.mark.asyncio
async def test_replay_resolves_claim_check_without_llm_call():
    class Store:
        def __init__(self):
            self.data = {}

        async def write_artifact(self, *, content: bytes, content_hash: str) -> str:
            ref = f"memory://{content_hash}"
            self.data[ref] = content
            return ref

        async def read_artifact(self, artifact_ref: str) -> bytes:
            return self.data[artifact_ref]

    store = Store()
    sealed = await seal_execution_artifact({"large": "z" * 5000}, artifact_store=store, inline_limit_bytes=64)

    details = {
        "provider": "test",
        "model": "none",
        "prompt_hash": "p",
        "system_prompt_hash": "s",
        "output_hash": sealed.content_hash,
        "tokens": 0,
        "cost_cents": 0,
        "fallback_used": False,
        "output": sealed.to_event_payload(),
    }

    output = await replay_captured_completion(details, artifact_store=store)
    assert output == {"large": "z" * 5000}
