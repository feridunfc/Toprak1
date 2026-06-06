from __future__ import annotations

import json
import os

import pytest

import scripts.worker_stream_consume_loop as stream_proof


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6389/0")


@pytest.mark.asyncio
async def test_worker_stream_consume_loop_passes_through_stream_loop() -> None:
    artifact = await stream_proof.build_artifact(REDIS_URL)

    if artifact["status"] == "DEGRADED":
        pytest.skip("; ".join(artifact["failing_reasons"]))

    assert artifact["status"] == "PASS"
    assert artifact["target_claim_supported"] is True
    assert artifact["redis_backend"] == "real_redis"

    assert artifact["production_lua_evalsha_path_used"] is True
    assert artifact["scheduler_lua_python_fallback_used"] is False
    assert artifact["dispatch_message_from_lua"] is True
    assert artifact["run_requested_event_written"] is True
    assert artifact["run_requested_event_consumed"] is True

    assert artifact["worker_stream_consume_loop_used"] is True
    assert artifact["direct_process_message_call_used"] is False
    assert artifact["worker_consumer_process_message_used"] is True

    assert artifact["idempotency_guard_claimed"] is True
    assert artifact["executor_invoked"] is True
    assert artifact["fake_executor_used"] is True
    assert artifact["state_store_result_written"] is True
    assert artifact["state_store_mark_completed_called"] is True
    assert artifact["message_acknowledged"] is True
    assert artifact["result_readable"] is True

    assert artifact["production_llm_call_attempted"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["operator_action_buttons"] is False
    assert artifact["noncanonical_redis_mutation_attempted"] is False
    assert artifact["failing_reasons"] == []


@pytest.mark.asyncio
async def test_worker_stream_consume_loop_degrades_without_redis() -> None:
    artifact = await stream_proof.build_artifact("redis://localhost:6399/0")

    assert artifact["status"] == "DEGRADED"
    assert artifact["target_claim_supported"] is False
    assert artifact["redis_backend"] == "unavailable"
    assert artifact["run_requested_event_consumed"] is False
    assert artifact["worker_stream_consume_loop_used"] is False


@pytest.mark.asyncio
async def test_worker_stream_consume_loop_writes_dashboard_artifact(tmp_path, monkeypatch) -> None:
    output = tmp_path / "latest_worker_stream_consume_loop.json"
    monkeypatch.setattr(stream_proof, "OUTPUT_PATH", output)

    artifact = await stream_proof.build_artifact(REDIS_URL)
    stream_proof.write_artifact(artifact)

    assert output.exists()
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["source"] == "worker_stream_consume_loop"

    if data["status"] == "PASS":
        assert data["target_claim_supported"] is True
        assert data["worker_stream_consume_loop_used"] is True
        assert data["direct_process_message_call_used"] is False
    else:
        assert data["status"] == "DEGRADED"
        assert data["target_claim_supported"] is False
