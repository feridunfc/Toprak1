from __future__ import annotations

import pytest

from scripts.runtime_payload_propagation import build_artifact

REDIS_URL = "redis://localhost:6389/0"


@pytest.mark.asyncio
async def test_runtime_payload_propagation_passes() -> None:
    message = "hello payload"

    artifact = await build_artifact(
        redis_url=REDIS_URL,
        message=message,
    )

    assert artifact["status"] == "PASS"
    assert artifact["target_claim_supported"] is True
    assert artifact["runtime_payload_propagated"] is True

    assert artifact["run_requested_payload"] == {"prompt": message}
    assert artifact["executor_payload"] == {"prompt": message}
    assert artifact["state_store_result_input"] == {"prompt": message}

    assert message in artifact["output_text"]
    assert "FAKE_RESPONSE: hello payload" in artifact["output_text"]

    assert artifact["product_result_normalization_used"] is False
    assert artifact["no_prompt_fallback_used"] is False
    assert artifact["submitted_message_patch_used"] is False

    assert artifact["production_lua_evalsha_path_used"] is True
    assert artifact["scheduler_lua_python_fallback_used"] is False
    assert artifact["worker_stream_consume_loop_used"] is True
    assert artifact["direct_process_message_call_used"] is False
    assert artifact["fake_executor_used"] is True
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["operator_action_buttons"] is False

    assert "no_prompt" not in artifact["output_text"]
    assert "submitted_message" not in artifact["output_text"]
    assert artifact["no_prompt_fallback_used"] is False
    assert artifact["submitted_message_patch_used"] is False


