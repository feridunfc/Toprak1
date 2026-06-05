from __future__ import annotations

import json
import os

import pytest

import scripts.production_lua_dispatch_path as lua_proof


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.mark.asyncio
async def test_production_lua_dispatch_path_passes_with_real_redis() -> None:
    artifact = await lua_proof.build_artifact(REDIS_URL)

    if artifact["status"] == "DEGRADED":
        pytest.skip("; ".join(artifact["failing_reasons"]))

    assert artifact["status"] == "PASS"
    assert artifact["redis_backend"] == "real_redis"
    assert artifact["target_claim_supported"] is True

    assert artifact["scheduler_lua_initialised"] is True
    assert artifact["dispatch_commit_loader_used"] is True
    assert artifact["dispatch_commit_sha_loaded"] is True
    assert artifact["dispatch_commit_sha"]
    assert artifact["production_lua_evalsha_path_used"] is True
    assert artifact["scheduler_lua_python_fallback_used"] is False

    assert artifact["tenant_task_submitted"] is True
    assert artifact["canonical_enqueue_used"] is True
    assert artifact["dispatch_committed"] is True
    assert artifact["dispatch_status"] == "committed"
    assert artifact["dispatch_output_created"] is True
    assert artifact["run_requested_event_from_lua_dispatch"] is True
    assert artifact["manual_worker_message_injection_used"] is False

    assert artifact["worker_consumer_process_message_used"] is True
    assert artifact["fake_executor_used"] is True
    assert artifact["worker_executor_invoked"] is True
    assert artifact["state_store_result_written"] is True
    assert artifact["state_store_transition_state_called"] is True
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
async def test_production_lua_dispatch_path_message_is_lua_dispatch_output() -> None:
    artifact = await lua_proof.build_artifact(REDIS_URL)

    if artifact["status"] == "DEGRADED":
        pytest.skip("; ".join(artifact["failing_reasons"]))

    message = artifact["dispatch_message"]
    assert message["event_type"] == "RunRequested"
    assert message["run_id"] == artifact["run_id"]
    assert message["tenant_id"] == artifact["tenant_id"]
    assert message["agent_type"] == "fake"
    assert message["worker_group"] == "default"
    assert message["shard"] == "0"


@pytest.mark.asyncio
async def test_production_lua_dispatch_path_degrades_without_redis() -> None:
    artifact = await lua_proof.build_artifact("redis://localhost:6399/0")

    assert artifact["status"] == "DEGRADED"
    assert artifact["redis_backend"] == "unavailable"
    assert artifact["target_claim_supported"] is False
    assert artifact["production_lua_evalsha_path_used"] is False
    assert artifact["scheduler_lua_python_fallback_used"] is False
    assert artifact["failing_reasons"]


@pytest.mark.asyncio
async def test_production_lua_dispatch_path_writes_dashboard_artifact(tmp_path, monkeypatch) -> None:
    output = tmp_path / "latest_production_lua_dispatch_path.json"
    monkeypatch.setattr(lua_proof, "OUTPUT_PATH", output)

    artifact = await lua_proof.build_artifact(REDIS_URL)
    lua_proof.write_artifact(artifact)

    assert output.exists()
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["source"] == "production_lua_dispatch_path"

    if data["status"] == "PASS":
        assert data["target_claim_supported"] is True
        assert data["production_lua_evalsha_path_used"] is True
        assert data["scheduler_lua_python_fallback_used"] is False
    else:
        assert data["status"] == "DEGRADED"
        assert data["target_claim_supported"] is False
