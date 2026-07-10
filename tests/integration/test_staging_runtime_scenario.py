
from __future__ import annotations

import json

import pytest

from scripts.staging_runtime_scenario import run_staging_runtime_scenario

pytestmark = pytest.mark.asyncio


async def test_staging_runtime_scenario_writes_pass_artifact(redis_client, tmp_path) -> None:
    artifact_path = tmp_path / "staging_runtime_scenario.json"

    artifact = await run_staging_runtime_scenario(
        redis_client,
        artifact_path=artifact_path,
    )

    assert artifact_path.exists()
    loaded = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert loaded == artifact
    assert artifact["status"] == "PASS"
    assert artifact["reason"] == "scenario_completed"

    assert artifact["task"]["task_id"] == "staging-runtime-scenario-task"
    assert artifact["task"]["run_id"] == "staging-runtime-scenario-run"
    assert artifact["task"]["task_id"] != artifact["task"]["run_id"]

    assert artifact["environment"]["redis_available"] is True
    assert artifact["environment"]["lua_available"] is True
    assert artifact["environment"]["bridge_flag_enabled"] is True
    assert artifact["environment"]["real_llm_called"] is False
    assert artifact["environment"]["deployment_attempted"] is False
    assert artifact["environment"]["release_tag_created"] is False

    assert artifact["runtime"]["stream_message_added"] is True
    assert artifact["runtime"]["message_entered_pending"] is True
    assert artifact["runtime"]["legacy_path_used"] is False
    assert artifact["runtime"]["task_consumer_called"] is True
    assert artifact["runtime"]["task_completed"] is True
    assert artifact["runtime"]["message_acknowledged_after_completion"] is True
    assert artifact["runtime"]["pending_before"] == 1
    assert artifact["runtime"]["pending_after"] == 0

    evidence = artifact["evidence"]
    assert evidence["found"] is True
    assert evidence["task_id"] == artifact["task"]["task_id"]
    assert evidence["run_id"] == artifact["task"]["run_id"]
    assert evidence["task_id"] != evidence["run_id"]
    assert evidence["state"] == "done"
    assert evidence["terminal_state"] == "done"
    assert evidence["worker_instance_id"] == artifact["task"]["worker_id"]
    assert evidence["scheduler_epoch"] == artifact["task"]["scheduler_epoch"]
    assert evidence["claim_epoch"] == "1"
    assert evidence["output_found"] is True
    assert evidence["output"]["task_id"] == artifact["task"]["task_id"]
    assert evidence["output"]["run_id"] == artifact["task"]["run_id"]

    assert artifact["safety"]["read_only_evidence_fetch"] is True
    assert artifact["safety"]["redis_mutation_attempted_by_evidence_reader"] is False
    assert artifact["safety"]["retry_or_reclaim_attempted"] is False
    assert artifact["safety"]["runtime_repair_attempted"] is False
    assert artifact["safety"]["production_ready_claim"] is False


class PingFailRedis:
    async def ping(self):
        raise RuntimeError("redis unavailable for scenario test")


async def test_staging_runtime_scenario_blocks_and_writes_artifact_when_redis_unavailable(tmp_path) -> None:
    artifact_path = tmp_path / "blocked_staging_runtime_scenario.json"

    artifact = await run_staging_runtime_scenario(
        PingFailRedis(),
        artifact_path=artifact_path,
    )

    assert artifact_path.exists()
    loaded = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert loaded == artifact
    assert artifact["status"] == "BLOCKED"
    assert "RuntimeError: redis unavailable for scenario test" in artifact["reason"]
    assert artifact["environment"]["redis_available"] is False
    assert artifact["environment"]["lua_available"] is False
    assert artifact["environment"]["deployment_attempted"] is False
    assert artifact["environment"]["release_tag_created"] is False
    assert artifact["environment"]["real_llm_called"] is False
    assert artifact["safety"]["production_ready_claim"] is False
